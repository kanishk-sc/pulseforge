"""Bounded local functional load measurement; never an SLA or capacity claim."""

import argparse
import asyncio
import json
import os
import platform
import random
import subprocess
import sys
import time
from collections import Counter
from datetime import UTC, datetime, timedelta
from pathlib import Path
from statistics import mean

import httpx
import psycopg

from pulseforge.config import Settings
from pulseforge.generator import EventGenerator


def percentile(values: list[float], rank: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * rank
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return round(ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower), 3)


def docker_stats() -> list[dict]:
    try:
        result = subprocess.run(
            ["docker", "stats", "--no-stream", "--format", "{{json .}}"],
            text=True,
            capture_output=True,
            timeout=20,
            check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        return [{"error": "docker stats unavailable"}]
    if result.returncode:
        return [{"error": "docker stats unavailable"}]
    return [json.loads(line) for line in result.stdout.splitlines() if line]


def environment_metadata() -> dict:
    def command(*parts: str) -> str | None:
        try:
            result = subprocess.run(parts, text=True, capture_output=True, timeout=10, check=False)
        except (OSError, subprocess.TimeoutExpired):
            return None
        return result.stdout.strip() if result.returncode == 0 else None

    docker = command("docker", "info", "--format", "{{json .}}")
    docker_info = json.loads(docker) if docker else {}
    return {
        "git_commit": command("git", "rev-parse", "HEAD"),
        "working_tree_clean": not bool(command("git", "status", "--porcelain")),
        "python_version": platform.python_version(),
        "docker_server_version": docker_info.get("ServerVersion"),
        "docker_logical_cpus": docker_info.get("NCPU"),
        "docker_memory_bytes": docker_info.get("MemTotal"),
        "warehouse_rows_at_start": warehouse_rows(),
    }


def warehouse_rows() -> int:
    settings = Settings()
    with psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=3,
    ) as connection:
        return connection.execute("SELECT count(*) FROM stream_events").fetchone()[0]


def expected_event_ids(seed: int, count: int) -> list[str]:
    generator = EventGenerator(seed=seed, scenario="normal", anomaly_rate=0)
    return [json.loads(generator.next_record()[1])["event_id"] for _ in range(count)]


def warehouse_rows_for_event_ids(event_ids: list[str]) -> int:
    settings = Settings()
    with psycopg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        dbname=settings.postgres_db,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        connect_timeout=3,
    ) as connection:
        return connection.execute(
            "SELECT count(*) FROM stream_events WHERE event_id = ANY(%s::uuid[])",
            (event_ids,),
        ).fetchone()[0]


def producer_environment(rate: float, seed: int) -> dict[str, str]:
    environment = os.environ.copy()
    environment["EVENTS_PER_SECOND"] = str(rate)
    environment["GENERATOR_SEED"] = str(seed)
    # The clean ingestion measurement expects one unique warehouse row per send.
    # Contract-rejection and duplicate behavior have separate integration tests.
    environment["ANOMALY_RATE"] = "0"
    return environment


async def api_load(args) -> dict:
    end = datetime.now(UTC).replace(minute=0, second=0, microsecond=0) + timedelta(hours=1)
    start = end - timedelta(days=1)
    params = {"start": start.isoformat(), "end": end.isoformat()}
    statuses: Counter = Counter()
    cache: Counter = Counter()
    latencies: list[float] = []
    semaphore = asyncio.Semaphore(args.concurrency)
    async with httpx.AsyncClient(base_url=args.url, timeout=10) as client:
        if args.scenario == "warm":
            warmup = await client.get("/api/v1/revenue", params={**params, "limit": 1000})
            if warmup.status_code != 200:
                raise RuntimeError(f"warm-up failed: HTTP {warmup.status_code}")

        async def one(index: int) -> None:
            await asyncio.sleep(index / args.request_rate)
            async with semaphore:
                limit = index + 1 if args.scenario == "cold" else 1000
                request_params = {**params, "limit": limit}
                began = time.perf_counter()
                try:
                    response = await client.get("/api/v1/revenue", params=request_params)
                except httpx.HTTPError:
                    statuses["transport_error"] += 1
                    return
                latencies.append((time.perf_counter() - began) * 1000)
                statuses[str(response.status_code)] += 1
                cache[response.headers.get("X-Cache", "none")] += 1

        began = time.perf_counter()
        await asyncio.gather(*(one(index) for index in range(args.requests)))
        elapsed = time.perf_counter() - began
    return {
        "scenario": args.scenario,
        "configuration": {
            "requests": args.requests,
            "concurrency": args.concurrency,
            "scheduled_request_rate_per_second": args.request_rate,
            "window_start_utc": start.isoformat(),
            "window_end_utc": end.isoformat(),
            "warmup_requests": int(args.scenario == "warm"),
            "cold_key_strategy": "unique bounded limit parameter"
            if args.scenario == "cold"
            else None,
        },
        "measured": {
            "duration_seconds": round(elapsed, 3),
            "throughput_requests_per_second": round(args.requests / elapsed, 3),
            "statuses": dict(statuses),
            "cache_headers": dict(cache),
            "scenario_condition_met": cache[
                {"warm": "hit", "cold": "miss", "fallback": "bypass"}[args.scenario]
            ]
            == args.requests,
            "error_count": args.requests - statuses["200"],
            "p50_ms": percentile(latencies, 0.5),
            "p95_ms": percentile(latencies, 0.95),
            "p99_ms": percentile(latencies, 0.99),
            "mean_ms": round(mean(latencies), 3) if latencies else None,
        },
    }


def ingestion_load(args) -> dict:
    before = warehouse_rows()
    seed = random.SystemRandom().randint(1, 2**31 - 1)
    event_ids = expected_event_ids(seed, args.events)
    if warehouse_rows_for_event_ids(event_ids):
        raise RuntimeError("generated event IDs already exist in the warehouse")
    environment = producer_environment(args.rate, seed)
    began = time.monotonic()
    result = subprocess.run(
        [sys.executable, "-m", "pulseforge.producer", "--count", str(args.events)],
        env=environment,
        text=True,
        capture_output=True,
        timeout=args.events / args.rate + 60,
        check=False,
    )
    producer_finished = time.monotonic()
    if result.returncode:
        raise RuntimeError(
            f"producer failed with exit code {result.returncode}; "
            f"stderr_tail={result.stderr[-500:]}"
        )
    deadline = time.monotonic() + 90
    observed = warehouse_rows_for_event_ids(event_ids)
    while observed < args.events and time.monotonic() < deadline:
        time.sleep(2)
        observed = warehouse_rows_for_event_ids(event_ids)
    after = warehouse_rows()
    return {
        "scenario": "ingestion",
        "configuration": {
            "events": args.events,
            "rate_events_per_second": args.rate,
            "seed": seed,
            "anomaly_rate": 0,
        },
        "measured": {
            "producer_wall_seconds": round(producer_finished - began, 3),
            "warehouse_rows_before": before,
            "warehouse_rows_after": after,
            "new_committed_rows": after - before,
            "matching_run_event_rows": observed,
            "producer_end_to_warehouse_observation_seconds": round(
                time.monotonic() - producer_finished, 3
            ),
            "all_events_observed": observed == args.events,
        },
        "interpretation": (
            "Acceptance checks this run's deterministic event IDs; the warehouse-wide row "
            "delta can include other traffic. Drain observation includes two-second polling "
            "and is not per-event processing latency. Event-time delay is not measured."
        ),
    }


def acceptance_ok(outcome: dict) -> bool:
    measured = outcome["measured"]
    if outcome["scenario"] == "ingestion":
        return measured["all_events_observed"]
    return measured["error_count"] == 0 and measured["scenario_condition_met"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--scenario", choices=("warm", "cold", "fallback", "ingestion"), required=True
    )
    parser.add_argument("--requests", type=int, default=50)
    parser.add_argument("--concurrency", type=int, default=4)
    parser.add_argument("--request-rate", type=float, default=20)
    parser.add_argument("--events", type=int, default=30)
    parser.add_argument("--rate", type=float, default=5)
    parser.add_argument("--url", default="http://127.0.0.1:8000")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if not 1 <= args.requests <= 500 or not 1 <= args.concurrency <= 32:
        parser.error("requests must be 1..500 and concurrency 1..32")
    if not 0 < args.request_rate <= 100:
        parser.error("request-rate must be 0..100/s")
    if not 1 <= args.events <= 500 or not 0 < args.rate <= 100:
        parser.error("events must be 1..500 and rate 0..100/s")
    started = datetime.now(UTC)
    environment = environment_metadata()
    before = docker_stats()
    outcome = ingestion_load(args) if args.scenario == "ingestion" else asyncio.run(api_load(args))
    output = {
        "measured_at_utc": started.isoformat(),
        "hardware": {"platform": platform.platform(), "logical_cpus": os.cpu_count()},
        "environment": environment,
        "docker_stats_before": before,
        "docker_stats_after": docker_stats(),
        **outcome,
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(output, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"scenario": args.scenario, "measured": outcome["measured"]}))
    if not acceptance_ok(outcome):
        raise SystemExit("scenario acceptance failed; inspect the JSON artifact")


if __name__ == "__main__":
    main()
