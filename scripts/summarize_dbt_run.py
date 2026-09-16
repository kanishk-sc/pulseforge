"""Emit a compact, machine-readable summary of a completed dbt invocation."""

import argparse
import json
from collections import Counter
from pathlib import Path


def summarize(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("dbt results must be an object")
    results = payload.get("results")
    if not isinstance(results, list) or not results:
        raise ValueError("dbt run_results.json does not contain a nonempty results list")
    if any(not isinstance(result, dict) for result in results):
        raise ValueError("dbt result entries must be objects")
    statuses = Counter(str(result.get("status", "unknown")) for result in results)
    failed = sum(count for status, count in statuses.items() if status not in {"pass", "success"})
    return {
        "invocation_id": payload.get("metadata", {}).get("invocation_id"),
        "result_count": len(results),
        "statuses": dict(sorted(statuses.items())),
        "failed": failed,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize dbt run_results.json")
    parser.add_argument("run_results", type=Path)
    args = parser.parse_args()
    summary = summarize(args.run_results)
    print(json.dumps(summary, sort_keys=True))
    if summary["failed"]:
        raise SystemExit("dbt results contain failures")


if __name__ == "__main__":
    main()
