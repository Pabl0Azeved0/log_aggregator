.PHONY: install test smoke format up down logs loadgen

install:
	python3 -m venv .venv
	.venv/bin/pip install -r requirements.txt

test:
	.venv/bin/pytest -q

smoke:
	.venv/bin/pytest -q tests/test_pipeline_offline.py

format:
	.venv/bin/black src scripts tests

up:
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

# Generates the README performance numbers — run with the compose stack up.
loadgen:
	.venv/bin/python scripts/loadgen.py --rate 5000 --duration 30 --query-url http://localhost:8080
