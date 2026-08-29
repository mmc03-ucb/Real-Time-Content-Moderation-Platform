#!/usr/bin/env bash
# Kills a worker in the middle of a load test.
#
# Kafka hands the dead worker's partitions to the survivor, which replays
# anything that was in flight. The run passes when the load test reports zero
# messages unaccounted for.

set -euo pipefail
cd "$(dirname "$0")/.."

PY=.venv/bin/python
RATE=${RATE:-1000}
DURATION=${DURATION:-20}

echo "starting two workers"
$PY -m moderation.pipeline.worker > /tmp/streamguard-worker-1.log 2>&1 &
WORKER_1=$!
$PY -m moderation.pipeline.worker > /tmp/streamguard-worker-2.log 2>&1 &
WORKER_2=$!
sleep 6   # let them join the consumer group

cleanup() { kill $WORKER_1 $WORKER_2 2>/dev/null || true; }
trap cleanup EXIT

echo "starting load"
$PY -m moderation.loadtest.bench --rate "$RATE" --duration "$DURATION" &
BENCH=$!

sleep $(( DURATION / 2 ))
echo "killing worker $WORKER_1 mid-run"
kill -9 $WORKER_1 || true

wait $BENCH
echo
echo "if 'unaccounted for' is 0, no message was lost when the worker died"
