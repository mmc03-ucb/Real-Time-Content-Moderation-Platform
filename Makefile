# StreamGuard — everything you need to run the platform.
# `make help` lists the targets.

PY := .venv/bin/python
WORKERS ?= 2

.PHONY: help install up down logs test worker workers api demo bench chaos reset

help:  ## Show this list
	@grep -E '^[a-z-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  %-10s %s\n", $$1, $$2}'

install:  ## Create the virtualenv and install dependencies
	python3 -m venv .venv
	$(PY) -m pip install --upgrade pip
	$(PY) -m pip install -r requirements.txt

up:  ## Start Kafka, Redis, MySQL, Prometheus and Grafana
	docker compose up -d

down:  ## Stop everything
	docker compose down

logs:  ## Tail the container logs
	docker compose logs -f

test:  ## Run the test suite with coverage
	$(PY) -m pytest --cov=moderation --cov-report=term-missing

worker:  ## Run one moderation worker
	$(PY) -m moderation.pipeline.worker

workers:  ## Run several workers at once (make workers WORKERS=4)
	@for i in $$(seq 1 $(WORKERS)); do \
		$(PY) -m moderation.pipeline.worker & \
	done; wait

api:  ## Serve the moderator dashboard on http://localhost:8000
	$(PY) -m uvicorn moderation.api.app:app --host 0.0.0.0 --port 8000

demo:  ## 60 seconds of chat with a raid 20 seconds in
	$(PY) -m moderation.ingest.loadgen --streams 20 --rate 500 --duration 60 \
		--toxic-ratio 0.08 --raid-at 20

bench:  ## Measure throughput and latency
	$(PY) -m moderation.loadtest.bench --rate 2000 --duration 20

chaos:  ## Kill a worker mid-run and check nothing was lost
	./scripts/chaos_test.sh

reset:  ## Wipe all data and start clean
	docker compose down -v
	docker compose up -d
