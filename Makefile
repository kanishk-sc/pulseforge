.PHONY: setup up down traffic streaming lint test integration
setup:
	uv sync --frozen
	uv run python scripts/init_env.py
up:
	docker compose up -d --build
down:
	docker compose down
traffic:
	docker compose --profile traffic up -d producer
streaming:
	docker compose --profile streaming up -d --build spark
lint:
	uv run ruff format --check .
	uv run ruff check .
test:
	uv run pytest -m "not integration"
integration:
	uv run pytest -m integration --run-integration
