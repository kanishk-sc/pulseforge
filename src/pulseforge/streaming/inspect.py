"""Host-side inspection honoring committed lake manifests (`uv run python -m ...inspect`)."""

import argparse
import json
from contextlib import closing

from pulseforge.dependencies import s3_client
from pulseforge.streaming.config import StreamSettings


def object_keys(storage, bucket: str, prefix: str) -> list[str]:
    return [
        item["Key"]
        for page in storage.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix)
        for item in page.get("Contents", [])
    ]


def read_bytes(storage, bucket: str, key: str) -> bytes:
    response = storage.get_object(Bucket=bucket, Key=key)
    with response["Body"] as body:
        return body.read()


def layer_files(storage, settings: StreamSettings, layer: str) -> list[str]:
    bucket = settings.s3_bucket
    paths = []
    if layer == "raw":
        logs = object_keys(storage, bucket, f"raw/{settings.stream_namespace}/_spark_metadata/")
        compact = [
            int(key.rsplit("/", 1)[-1].split(".")[0]) for key in logs if key.endswith(".compact")
        ]
        base = max(compact, default=-1)
        for key in logs:
            name = key.rsplit("/", 1)[-1]
            if not name.split(".")[0].isdigit():
                continue
            number = int(name.split(".")[0])
            if number < base or (number == base and not name.endswith(".compact")):
                continue
            for line in read_bytes(storage, bucket, key).decode().splitlines()[1:]:
                entry = json.loads(line)
                if entry["action"] == "add":
                    paths.append(entry["path"].removeprefix(f"s3a://{bucket}/"))
    elif layer in {"cleaned", "curated"}:
        for key in object_keys(storage, bucket, f"commits/{settings.stream_namespace}/"):
            manifest = json.loads(read_bytes(storage, bucket, key))
            if manifest["row_count"]:
                prefix = manifest[layer].removeprefix(f"s3a://{bucket}/") + "/"
                paths.extend(
                    key
                    for key in object_keys(storage, bucket, prefix)
                    if key.endswith(".parquet") and "/_temporary/" not in key
                )
    else:
        raise ValueError("layer must be raw, cleaned or curated")
    return sorted(set(paths))


def read_layer(layer: str) -> list[dict]:
    import pyarrow as pa
    import pyarrow.parquet as pq

    settings = StreamSettings()
    with closing(s3_client(settings)) as storage:
        return [
            row
            for key in layer_files(storage, settings, layer)
            for row in pq.read_table(
                pa.BufferReader(read_bytes(storage, settings.s3_bucket, key))
            ).to_pylist()
        ]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Inspect committed synthetic lake records")
    parser.add_argument("--layer", choices=["raw", "cleaned", "curated"], default="curated")
    parser.add_argument("--limit", type=int, default=5)
    args = parser.parse_args()
    if args.limit < 0:
        parser.error("limit must be nonnegative")
    rows = read_layer(args.layer)
    print(
        json.dumps(
            {"layer": args.layer, "row_count": len(rows), "sample": rows[: args.limit]},
            default=str,
            indent=2,
        )
    )
