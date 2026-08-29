# StreamGuard

Real-time content moderation for live stream chat, built on the stack a Trust &
Safety team actually runs: Python, Kafka, Redis and MySQL.

Chat arrives as a firehose. StreamGuard reads it, decides what to do with every
message in a few milliseconds, removes the clear violations on its own, and puts
only the genuinely unclear cases in front of a human.

---

## What it does

```
 ┌──────────────┐   ┌──────────────┐   ┌────────────────────┐   ┌──────────────┐
 │ Chat load    │──▶│ Kafka        │──▶│ Moderation workers │──▶│ moderation.  │
 │ generator    │   │ chat.events  │   │  · dedup           │   │ decisions    │
 │ (simulated   │   │ (6 parts,    │   │  · rate limit      │   └──────┬───────┘
 │  firehose)   │   │  keyed by    │   │  · raid check      │          │
 └──────────────┘   │  stream_id)  │   │  · rules           │          ▼
                    └──────────────┘   │  · toxicity model  │   ┌──────────────┐
                                       └─────────┬──────────┘   │ MySQL        │
                    ┌──────────┐                 │              │ decisions,   │
                    │ Redis    │◀────────────────┤              │ audit trail  │
                    │ · limits │                 │              └──────────────┘
                    │ · dedup  │                 ▼
                    │ · risk   │        ┌──────────────────┐    ┌──────────────┐
                    │ · raids  │        │ review.queue     │───▶│ FastAPI      │
                    └──────────┘        │ (unclear cases)  │    │ dashboard    │
                                        └──────────────────┘    └──────────────┘

           Prometheus + Grafana + OpenTelemetry watching all of it
```

---

## Run it

```bash
make install     # virtualenv and dependencies
make up          # Kafka, Redis, MySQL, Prometheus, Grafana
make workers WORKERS=2
make api         # dashboard on http://localhost:8000
make demo        # a minute of chat, with a raid 20 seconds in
```

Then look at:

| | |
|---|---|
| Moderator queue | http://localhost:8000 |
| Grafana dashboard | http://localhost:3000 |
| API docs | http://localhost:8000/docs |

Other useful targets: `make test`, `make bench`, `make chaos`, `make reset`.

---

## What happens to one message

Each stage is cheaper than the one after it, so most traffic is settled before
anything expensive runs.

| # | Stage | Catches | Cost |
|---|---|---|---|
| 1 | Duplicate check | Copy-paste spam: same user, same text, seconds apart | one Redis call |
| 2 | Rate limit | Someone flooding the chat | one Redis call |
| 3 | Raid check | A crowd of new accounts hitting one stream at once | one Redis call |
| 4 | Rules | Slurs, scam phrases, link policy, new-account limits | in memory, no I/O |
| 5 | Toxicity model | Everything a pattern cannot describe | one call per batch |

Whatever survives all five is scored 0 to 1 and lands in one of three buckets:

- **above 0.9** — removed automatically
- **0.6 to 0.9** — sent to the human review queue
- **below 0.6** — allowed

Those thresholds come from the strategy the stream is assigned to, and drop
further for users with a recent history of violations.

---

## Measured on a laptop

Two workers, one Kafka broker, everything in Docker on one machine.

| Workers | Sustained throughput | End to end p99 | Messages lost |
|---|---|---|---|
| 2 | 2,700 msg/s | 193 ms | 0 |
| 4 | 8,300 msg/s | 170 ms | 0 |

Two more numbers worth having:

- **Rule changes reach every worker in 3 seconds**, with no restart. Measured by
  toggling a rule in the dashboard and watching the version each worker reports.
- **Killing a worker mid-run loses nothing.** The survivor picks up the dead
  worker's partitions and replays what was in flight. The messages caught in the
  handover are about 12 seconds late; everything else is unaffected.

Throughput is what the workers kept up with while staying under 200ms, read off
the workers' own metrics. The 2 worker row is limited by the load generator, not
by the pipeline: one Python producer tops out around 2,700 messages a second.

`make bench` reproduces these. It listens on the decisions topic while it
produces, so the numbers come out of the pipeline itself.

---

## Design decisions

**Rules before the model.** Pattern matching costs nothing and settles most
traffic. Only what is genuinely ambiguous is worth a model call, and that is
where the whole throughput story comes from.

**One call per batch, everywhere.** Workers pull up to 500 messages at a time
(or whatever has arrived in 100ms) and then talk to each dependency once for the
whole batch: one Redis round trip for the rate limits, duplicates, raid counters
and reputation lookups; one call to the model; one insert into MySQL. Doing that
per message instead was the difference between roughly 700 and 4,000 messages a
second per worker.

**Partition by stream_id.** One stream's messages always land on the same
partition, so they stay in order relative to each other. Different streams are
free to be handled in parallel, which is what lets workers scale out.

**At-least-once, with idempotent writes.** Offsets are committed only after a
batch is safely in MySQL, so a crash means messages are handled again rather
than lost. `decisions.msg_id` is unique, so handling one twice records it once.

**Rules in MySQL, version in Redis.** Workers hold a ruleset in memory and watch
one number in Redis. Change a rule in the dashboard and every worker picks it up
within seconds, with no restarts. Every decision stores the rule id and version
behind it, so any removal can be explained months later.

**Hidden, not escalated, during a raid.** A raid could push thousands of
messages into the review queue and bury the moderators. New accounts are quietly
held back instead until the stream settles.

**Escalate rather than guess.** The middle band goes to a person. Their answers
are stored as labels, which is what makes tuning the thresholds an evidence
question instead of an argument.

---

## A/B testing moderation policy

Every stream is hashed into bucket A or B, each with its own thresholds. A is
the current policy; B trusts the model more. Decisions carry their bucket, so
the dashboard can compare, per strategy:

- how much was removed automatically
- how much review work was created
- how often a human overturned the call (our stand-in for a false positive)

That turns "should we lower the threshold?" into a question with an answer.

---

## When things break

Worker crashes, a dead model, workers falling behind, unreadable messages, bad
rule changes — each one has a designed behaviour, written down in
[docs/RELIABILITY.md](docs/RELIABILITY.md).

`make chaos` kills a worker halfway through a load test and shows that nothing
was lost.

---

## The toxicity model

The pipeline scores text through a swappable backend:

- **Detoxify**, a real pretrained toxicity model, used automatically if the
  optional `detoxify` package is installed.
- **A built-in word-list scorer** otherwise, so the project runs anywhere with
  no model download.

Either way the pipeline sees the same thing: a list of messages in, a list of
scores out, one call per batch.

---

## Layout

```
moderation/
  ingest/      chat generator, Kafka producer and consumer
  rules/       rule definitions, the matching engine, hot reload
  defenses/    Redis: rate limits, dedup, risk scores, raid detection
  ml/          toxicity scoring, with a fallback when the model is down
  pipeline/    the funnel, the worker process, where verdicts get written
  strategies/  A/B buckets and their thresholds
  api/         FastAPI review queue and the moderator dashboard
  obs/         Prometheus metrics and OpenTelemetry traces
  loadtest/    throughput and latency measurement
sql/           schema and starting ruleset
grafana/       provisioned dashboard
tests/         the test suite
```

---

## Tests

```bash
make test
```

Everything runs without Kafka, Redis or MySQL: the tests use an in-memory Redis
and stand-ins for Kafka and the database, so the whole suite finishes in under a
second.
