"""
Fake live-stream chat firehose.

Produces a mix of normal chat, spam, toxic messages and, optionally, a scripted
"raid" where a crowd of brand new accounts floods one stream at once.

    python -m moderation.ingest.loadgen --streams 20 --rate 2000 --toxic-ratio 0.05
"""

import argparse
import random
import time
from typing import Optional

from moderation.ingest import corpus
from moderation.ingest.generator import make_message


class ChatSimulator:
    """Builds messages. Knows nothing about Kafka, so it is easy to test."""

    def __init__(self,
                 streams: int = 10,
                 users_per_stream: int = 200,
                 toxic_ratio: float = 0.05,
                 spam_ratio: float = 0.05,
                 link_ratio: float = 0.05,
                 seed: Optional[int] = None,
                 prefix: str = "stream"):
        self.prefix = prefix
        self.stream_ids = [f"{prefix}_{i}" for i in range(streams)]
        self.users_per_stream = users_per_stream
        self.toxic_ratio = toxic_ratio
        self.spam_ratio = spam_ratio
        self.link_ratio = link_ratio
        self.rng = random.Random(seed)
        # Streams currently being raided, and the counter used to name the
        # throwaway accounts doing the raiding.
        self.raiding: set = set()
        self._raid_account = 0

    def start_raid(self, stream_id: str) -> None:
        self.raiding.add(stream_id)

    def stop_raid(self, stream_id: str) -> None:
        self.raiding.discard(stream_id)

    def next_message(self) -> dict:
        """Pick a stream, a user and a line of text, weighted by the ratios."""
        stream_id = self.rng.choice(self.stream_ids)

        # During a raid most of the traffic comes from fresh accounts that
        # all shout at the same stream.
        if self.raiding and self.rng.random() < 0.7:
            stream_id = self.rng.choice(sorted(self.raiding))
            self._raid_account += 1
            return make_message(
                user_id=f"raider_{self._raid_account}",
                text=self.rng.choice(corpus.TOXIC + corpus.SPAM),
                stream_id=stream_id,
                client_meta={"account_age_days": 0, "platform": "web"},
            )

        user_id = f"viewer_{self.rng.randrange(self.users_per_stream)}"
        roll = self.rng.random()
        if roll < self.toxic_ratio:
            text = self.rng.choice(corpus.TOXIC)
        elif roll < self.toxic_ratio + self.spam_ratio:
            # Spammers repeat themselves, which is what the dedup check catches.
            text = self.rng.choice(corpus.SPAM)
        elif roll < self.toxic_ratio + self.spam_ratio + self.link_ratio:
            text = self.rng.choice(corpus.LINKS)
        else:
            text = self.rng.choice(corpus.NORMAL)

        return make_message(
            user_id=user_id,
            text=text,
            stream_id=stream_id,
            client_meta={"account_age_days": self.rng.randrange(0, 900), "platform": "web"},
        )


def run(sim: ChatSimulator, publish_one, rate: int, duration: float,
        raid_at: Optional[float] = None, raid_seconds: float = 15.0) -> int:
    """
    Send `rate` messages a second for `duration` seconds.

    Work is done in small slices so the pace stays even instead of arriving in
    one lump every second.
    """
    started = time.perf_counter()
    slice_seconds = 0.05
    per_slice = max(1, int(rate * slice_seconds))
    sent = 0
    raid_stream = None

    while True:
        elapsed = time.perf_counter() - started
        if elapsed >= duration:
            break

        if raid_at is not None and raid_stream is None and elapsed >= raid_at:
            raid_stream = sim.stream_ids[0]
            sim.start_raid(raid_stream)
            print(f"raid started on {raid_stream}")
        if raid_stream and elapsed >= (raid_at + raid_seconds):
            sim.stop_raid(raid_stream)
            print(f"raid over on {raid_stream}")
            raid_stream = None

        slice_end = time.perf_counter() + slice_seconds
        for _ in range(per_slice):
            publish_one(sim.next_message())
            sent += 1
        sleep_for = slice_end - time.perf_counter()
        if sleep_for > 0:
            time.sleep(sleep_for)

    return sent


def main() -> None:
    parser = argparse.ArgumentParser(description="StreamGuard chat firehose")
    parser.add_argument("--streams", type=int, default=10)
    parser.add_argument("--rate", type=int, default=1000, help="messages per second")
    parser.add_argument("--duration", type=float, default=30.0, help="seconds")
    parser.add_argument("--toxic-ratio", type=float, default=0.05)
    parser.add_argument("--spam-ratio", type=float, default=0.05)
    parser.add_argument("--raid-at", type=float, default=None,
                        help="seconds into the run to start a raid")
    parser.add_argument("--seed", type=int, default=None)
    args = parser.parse_args()

    # Imported here so tests can use the simulator without Kafka installed.
    from moderation.ingest.producer import build_producer, flush, publish

    producer = build_producer()
    sim = ChatSimulator(streams=args.streams, toxic_ratio=args.toxic_ratio,
                        spam_ratio=args.spam_ratio, seed=args.seed)

    started = time.perf_counter()
    sent = run(sim, lambda m: publish(producer, m), args.rate, args.duration,
               raid_at=args.raid_at)
    flush(producer, timeout=30)
    took = time.perf_counter() - started
    print(f"sent {sent} messages in {took:.1f}s ({sent / took:.0f}/s)")


if __name__ == "__main__":
    main()
