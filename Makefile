# Convenience targets for macOS, Linux and WSL. On Windows without make, run the commands
# shown in README.md directly (they are the same `uv run ...` calls).

COMPOSE := docker compose -f deploy/docker-compose.yml
DOMAIN ?= example.org
CRAWL ?= latest
LIMIT ?= 50
BUDGET ?= 60
TEST_PG_DSN ?= postgresql://strata:strata@localhost:5432/strata_ops

.PHONY: up down migrate lake-init check test demo clean

up:
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

migrate:
	uv run strata migrate

lake-init:
	uv run strata lake-init

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run mypy

test:
	STRATA_TEST_PG_DSN=$(TEST_PG_DSN) uv run pytest

demo: up
	uv run strata demo --domain $(DOMAIN) --crawl $(CRAWL) --limit $(LIMIT) --budget $(BUDGET)

clean:
	rm -rf data .pytest_cache .mypy_cache .ruff_cache .hypothesis
