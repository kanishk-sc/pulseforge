FROM maven:3.9.11-eclipse-temurin-17 AS connectors
WORKDIR /build
COPY infra/docker/spark-pom.xml pom.xml
RUN mvn -B -q org.apache.maven.plugins:maven-dependency-plugin:3.8.1:copy-dependencies -DoutputDirectory=/jars

FROM ghcr.io/astral-sh/uv:0.11.16 AS uv
FROM python:3.12-slim-bookworm
RUN apt-get update && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/*
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV PYTHONUNBUFFERED=1 PATH="/app/.venv/bin:$PATH" PYSPARK_PYTHON=/app/.venv/bin/python
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --extra streaming --no-install-project
COPY --from=connectors /jars /opt/connectors
COPY src ./src
RUN uv sync --frozen --no-dev --extra streaming && useradd --uid 10001 --create-home pulseforge
USER 10001
CMD ["python", "-m", "pulseforge.streaming"]
