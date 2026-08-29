"""
Load test.

Pushes a known amount of traffic through the real pipeline and reports what came
out the other end: throughput, end to end latency, and whether every message was
accounted for.

    python -m moderation.loadtest.bench --rate 2000 --duration 30

It listens on the decisions topic while it produces, so the numbers come from
the pipeline itself rather than from a stopwatch around it.
"""

import argparse
import json
import math
import statistics
import time
import uuid

from moderation.config import settings
from moderation.ingest.consumer import build_consumer, parse
from moderation.ingest.loadgen import ChatSimulator, run
from moderation.ingest.producer import build_producer, flush, publish


def percentile(values, fraction: float) -> float:
    """Nearest rank percentile: p99 is the value 99% of messages beat."""
    if not values:
        return 0.0
    ordered = sorted(values)
    rank = math.ceil(fraction * len(ordered))
    return ordered[min(len(ordered), max(1, rank)) - 1]


def collect_decisions(consumer, prefix: str, expected: int,
                      timeout: float = 30.0):
    """
    Read verdicts until we have one per message sent, or we give up waiting.

    Also returns how long the verdicts took to arrive, which is the rate the
    pipeline drains a backlog at, and whether we ran out of patience.
    """
    latencies, actions = [], {}
    started = time.perf_counter()
    deadline = started + timeout
    last_seen = started
    while len(latencies) < expected and time.perf_counter() < deadline:
        batch = consumer.consume(num_messages=500, timeout=0.5)
        for raw in batch:
            if raw.error():
                continue
            event = parse(raw.value())
            if not event.get("stream_id", "").startswith(prefix):
                continue  # traffic from some other run
            latencies.append(event["latency_ms"])
            actions[event["action"]] = actions.get(event["action"], 0) + 1
        if batch:
            last_seen = time.perf_counter()
    return latencies, actions, last_seen - started, len(latencies) < expected


def report(sent: int, produce_seconds: float, latencies, actions,
           settle_seconds: float, timed_out: bool = False) -> dict:
    decided = len(latencies)
    return {
        "sent": sent,
        "decided": decided,
        "missing": sent - decided,
        "timed_out": timed_out,
        "produced_per_second": round(sent / produce_seconds, 1),
        "decided_per_second": round(decided / settle_seconds, 1) if settle_seconds else 0,
        "p50_ms": round(percentile(latencies, 0.50), 1),
        "p95_ms": round(percentile(latencies, 0.95), 1),
        "p99_ms": round(percentile(latencies, 0.99), 1),
        "max_ms": round(max(latencies), 1) if latencies else 0,
        "mean_ms": round(statistics.fmean(latencies), 1) if latencies else 0,
        "actions": actions,
    }


def print_report(result: dict) -> None:
    print("\n--- StreamGuard load test ---")
    print(f"  sent                {result['sent']}")
    print(f"  decided             {result['decided']}")
    if result["timed_out"]:
        print(f"  still in flight     {result['missing']} (gave up waiting, not lost)")
    else:
        print(f"  unaccounted for     {result['missing']}")
    print(f"  produced            {result['produced_per_second']}/s")
    print(f"  moderated           {result['decided_per_second']}/s "
          f"(rate the workers cleared the backlog at)")
    print(f"  latency p50/p95/p99 {result['p50_ms']} / {result['p95_ms']} / "
          f"{result['p99_ms']} ms")
    print(f"  slowest message     {result['max_ms']} ms")
    print("  outcomes            " +
          ", ".join(f"{k} {v}" for k, v in sorted(result["actions"].items())))


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamGuard load test")
    parser.add_argument("--rate", type=int, default=1000, help="messages per second")
    parser.add_argument("--duration", type=float, default=20.0, help="seconds")
    parser.add_argument("--streams", type=int, default=20)
    parser.add_argument("--toxic-ratio", type=float, default=0.05)
    parser.add_argument("--settle-timeout", type=float, default=60.0,
                        help="how long to wait for the last verdicts")
    parser.add_argument("--json", action="store_true", help="print the raw numbers too")
    args = parser.parse_args()

    # Tag this run so its numbers cannot be mixed up with other traffic.
    prefix = f"bench{uuid.uuid4().hex[:6]}"

    consumer = build_consumer(group=f"bench-{prefix}")
    consumer.subscribe([settings.decisions_topic])
    consumer.consume(num_messages=1, timeout=5.0)  # join the group before producing

    producer = build_producer()
    sim = ChatSimulator(streams=args.streams, toxic_ratio=args.toxic_ratio,
                        prefix=prefix)

    print(f"producing {args.rate}/s for {args.duration}s across {args.streams} streams")
    started = time.perf_counter()
    sent = run(sim, lambda m: publish(producer, m), args.rate, args.duration)
    flush(producer, timeout=30)
    produce_seconds = time.perf_counter() - started

    latencies, actions, settle_seconds, timed_out = collect_decisions(
        consumer, prefix, sent, timeout=args.settle_timeout)
    consumer.close()

    result = report(sent, produce_seconds, latencies, actions, settle_seconds,
                    timed_out)
    print_report(result)
    if args.json:
        print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
