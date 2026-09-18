.PHONY: setup up down traffic streaming analytics lint test integration
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
analytics:
	docker compose --profile analytics run --rm dbt deps --profiles-dir .
	docker compose --profile analytics run --rm dbt build --profiles-dir . --target dev
lint:
	uv run ruff format --check .
	uv run ruff check .
test:
	uv run pytest -m "not integration"
integration:
	uv run pytest -m integration --run-integration
