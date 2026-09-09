"""Emit a compact, machine-readable summary of a completed dbt invocation."""

import argparse
import json
from collections import Counter
from pathlib import Path


def summarize(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    results = payload.get("results")
    if not isinstance(results, list):
        raise ValueError("dbt run_results.json does not contain a results list")
    statuses = Counter(str(result.get("status", "unknown")) for result in results)
    failed = sum(statuses[status] for status in ("error", "fail", "runtime error"))
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
