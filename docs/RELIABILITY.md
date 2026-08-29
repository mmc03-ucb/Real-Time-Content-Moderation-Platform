# Failure modes and what StreamGuard does about them

Moderation sits in front of a live audience. It has to keep making calls while
parts of it are broken. This is the list of things that go wrong and the
behaviour that was designed for each.

## A worker dies mid-batch

**What happens:** Kafka notices the worker is gone, gives its partitions to the
survivors, and they start again from the last committed offset.

**Why nothing is lost:** offsets are committed only after a batch has been fully
written to MySQL. A worker that dies halfway through a batch never told Kafka it
was done, so those messages are handed to another worker.

**Why nothing is doubled:** the redelivered messages get the same `msg_id`, and
`decisions.msg_id` is unique with `INSERT IGNORE`, so the second write is
silently dropped. At-least-once delivery plus idempotent writes gives
exactly-once results in the place that matters: the record of what was decided.

**How to see it:** `make chaos` kills a worker halfway through a load test. The
run reports `unaccounted for 0`.

## The toxicity model stops answering

**What happens:** every model call is wrapped so a failure returns "no score"
instead of raising. Messages that no rule caught are allowed through, the
`streamguard_model_healthy` gauge drops to 0, and the reason code on those
decisions is `ml_unavailable`.

**Why allow rather than block:** a broken model is our problem, not the
viewers'. Blocking a whole stream's chat because an internal service is down is
a bigger outage than briefly missing some borderline messages. The rules keep
running the entire time, so the clear cases are still caught. The gauge is what
gets someone paged.

## Workers cannot keep up

**What happens:** Kafka buffers. Nothing is dropped, but verdicts arrive later
and later.

**How you know:** `streamguard_consumer_lag` climbs steadily instead of hovering
near zero, and the end to end p99 panel rises with it.

**What to do:** start more workers. They join the same consumer group and Kafka
splits the partitions between them, so scaling out needs no config change and no
restart of anything already running. `chat.events` has six partitions, so up to
six workers share the load; past that, raise the partition count.

## A message arrives that we cannot read

**What happens:** it goes to the `chat.deadletter` topic with the error attached,
and the worker moves on.

**Why it matters:** one bad message must never be able to stall a partition. A
worker that crashes on it would restart, read it again, and crash again.

## MySQL or Redis is briefly unavailable

MySQL connections are retried on start up so containers can boot in any order.
If MySQL goes away mid-run the worker crashes and restarts, and Kafka replays
whatever was uncommitted. Redis is the hot path for rate limits, dedup and risk
scores; losing it means those defences are unavailable while the rules and the
model keep working.

## A rule change goes wrong

Rules are versioned. Every decision records the rule id and version that caused
it, so "why was this deleted on Tuesday?" has an exact answer. Turning a rule
off in the dashboard bumps a version number in Redis, and every worker picks up
the new ruleset within seconds without restarting.

## A raid hits a stream

A crowd of brand new accounts on one stream trips raid mode for that stream.
New accounts are hidden rather than escalated on purpose: sending thousands of
raid messages to the review queue would bury the moderators and stop real work.
Raid mode expires on its own after a minute of calm.
