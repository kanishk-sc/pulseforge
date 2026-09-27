FROM ghcr.io/astral-sh/uv:0.11.16 AS uv
FROM python:3.12-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV PYTHONUNBUFFERED=1 UV_COMPILE_BYTECODE=1 PATH="/app/.venv/bin:$PATH"
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src ./src
COPY docs/assistant/corpus.json ./docs/assistant/corpus.json
COPY docs/operations/runbooks.md ./docs/operations/runbooks.md
COPY docs/architecture/phase-4-design.md ./docs/architecture/phase-4-design.md
COPY docs/architecture/phase-5-design.md ./docs/architecture/phase-5-design.md
RUN uv sync --frozen --no-dev && useradd --uid 10001 --create-home pulseforge \
    && mkdir -p /app/.cache/pulseforge-model \
    && chown -R 10001:10001 /app/.cache
USER 10001
EXPOSE 8000
CMD ["uvicorn", "pulseforge.api:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]
