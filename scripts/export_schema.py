"""Export the public contract; --check rejects schema drift in CI."""

import argparse
import json
from pathlib import Path

from pulseforge.events import CommerceEvent

parser = argparse.ArgumentParser()
parser.add_argument("--check", action="store_true")
args = parser.parse_args()
destination = Path(__file__).resolve().parents[1] / "schemas" / "commerce-event.v1.json"
schema = json.dumps(CommerceEvent.model_json_schema(), indent=2, sort_keys=True) + "\n"
if args.check:
    if not destination.exists() or destination.read_text(encoding="utf-8") != schema:
        raise SystemExit("Schema drift: run uv run python scripts/export_schema.py")
    print("Version 1 schema matches the Pydantic contract.")
else:
    destination.parent.mkdir(exist_ok=True)
    destination.write_text(schema, encoding="utf-8")
    print("Exported schemas/commerce-event.v1.json")
