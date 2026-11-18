# Convenience targets for macOS, Linux and WSL. On Windows without make, run the commands
# shown in README.md directly (they are the same `uv run ...` calls).

COMPOSE := docker compose -f deploy/docker-compose.yml
DOMAIN ?= peps.python.org
CRAWL ?= latest
LIMIT ?= 20
# A fresh URL-index lookup alone costs about 50 requests; the CLI's own default is 150.
BUDGET ?= 150
TEST_PG_DSN ?= postgresql://strata:strata@localhost:5432/strata_ops

.PHONY: dev up down migrate lint check test demo clean

dev: up
	uv sync
	uv run strata migrate

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

migrate:
	uv run strata migrate

lint:
	uv run ruff check .
	uv run ruff format --check .

check: lint
	uv run mypy

test:
	STRATA_TEST_PG_DSN=$(TEST_PG_DSN) uv run pytest

demo: up
	uv run strata demo --domain $(DOMAIN) --crawl $(CRAWL) --limit $(LIMIT) --budget $(BUDGET)

clean:
	rm -rf data .pytest_cache .mypy_cache .ruff_cache .hypothesis
