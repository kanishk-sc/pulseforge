"""Assert live Prometheus, Grafana and Tempo behavior against populated services."""

import json
import subprocess
import time
from pathlib import Path
from uuid import uuid4

import httpx
from dotenv import dotenv_values


def logs_for(request_id: str) -> dict | None:
    result = subprocess.run(
        ["docker", "compose", "logs", "--no-color", "--tail", "300", "api"],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    for line in reversed(result.stdout.splitlines()):
        if request_id not in line:
            continue
        try:
            entry = json.loads(line[line.index("{") :])
        except (ValueError, json.JSONDecodeError):
            continue
        if entry.get("request_id") == request_id and entry.get("message") == "http_request":
            return entry
    return None


def eventually(function, seconds: int = 30):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        result = function()
        if result:
            return result
        time.sleep(1)
    raise AssertionError("live observability condition did not become true")


def main() -> None:
    with httpx.Client(timeout=5) as client:

        def required_targets():
            found = client.get("http://127.0.0.1:9090/api/v1/targets").raise_for_status()
            targets = found.json()["data"]["activeTargets"]
            states = {target["labels"]["job"]: target["health"] for target in targets}
            return (
                targets
                if all(states.get(job) == "up" for job in ("pulseforge-api", "pulseforge-ops"))
                else None
            )

        targets = eventually(required_targets)
        states = {target["labels"]["job"]: target["health"] for target in targets}
        assert states["pulseforge-api"] == "up", states
        assert states["pulseforge-ops"] == "up", states
        source = (
            client.get(
                "http://127.0.0.1:9090/api/v1/query",
                params={"query": "pulseforge_warehouse_committed_rows"},
            )
            .raise_for_status()
            .json()["data"]["result"]
        )
        assert source and float(source[0]["value"][1]) > 0, source

        password = dotenv_values(Path(__file__).resolve().parents[1] / ".env").get(
            "GRAFANA_ADMIN_PASSWORD"
        )
        assert password, "generate local Grafana credential first"
        dashboards = {}
        for uid in (
            "pulseforge-api-overview",
            "pulseforge-streaming",
            "pulseforge-analytics",
            "pulseforge-detectors",
        ):
            response = client.get(
                f"http://127.0.0.1:3000/api/dashboards/uid/{uid}",
                auth=("admin", password),
            )
            response.raise_for_status()
            dashboards[uid] = len(response.json()["dashboard"]["panels"])
            assert dashboards[uid] > 0

        trace_ids = []

        def fetch_trace():
            for trace_id in trace_ids:
                trace_response = client.get(f"http://127.0.0.1:3200/api/traces/{trace_id}")
                if trace_response.status_code == 404:
                    continue
                trace_response.raise_for_status()
                result = trace_response.json()
                if result.get("batches"):
                    names = {
                        span["name"]
                        for batch in result["batches"]
                        for scope in batch["scopeSpans"]
                        for span in scope["spans"]
                    }
                    if {"GET /api/v1/revenue", "postgres.latest_publication", "redis.get"} <= names:
                        return trace_id, result
            return None

        # The default root sampler retains only 10% of requests. Check in small
        # batches, with a finite upper bound, rather than assuming one is sampled.
        found = None
        for _ in range(10):
            for _ in range(10):
                request_id = f"phase5-{uuid4().hex}"
                response = client.get(
                    "http://127.0.0.1:8000/api/v1/revenue",
                    params={"limit": 77},
                    headers={"X-Request-ID": request_id},
                )
                assert response.status_code == 200, response.status_code
                assert response.headers["X-Request-ID"] == request_id
                log = eventually(lambda request_id=request_id: logs_for(request_id))
                assert log["path"] == "/api/v1/revenue", log
                assert log["trace_id"] and log["span_id"], log
                trace_ids.append(log["trace_id"])
            time.sleep(2)
            found = fetch_trace()
            if found:
                break
        if not found:
            found = eventually(fetch_trace)
        trace_id, trace = found
        spans = [
            span
            for batch in trace["batches"]
            for scope in batch["scopeSpans"]
            for span in scope["spans"]
        ]
        names = {span["name"] for span in spans}
        assert "GET /api/v1/revenue" in names, names
        assert "postgres.latest_publication" in names, names
        assert "redis.get" in names, names
        attributes = json.dumps(trace)
        assert "POSTGRES_PASSWORD" not in attributes
        assert "authorization" not in attributes.lower()
        print(
            json.dumps(
                {
                    "targets": states,
                    "warehouse_rows": float(source[0]["value"][1]),
                    "dashboards": dashboards,
                    "trace_id": trace_id,
                    "span_names": sorted(names),
                }
            )
        )


if __name__ == "__main__":
    main()
