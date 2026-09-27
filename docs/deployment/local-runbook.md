# Isolated local release runbook

Run from the repository root on Docker Engine/Compose with an x86_64 Linux
container platform, `uv`, and a clean reviewed Git commit. Do **not** use
the existing `pulseforge` Compose project or its volumes. The commands below
use a separate `pulseforge-p7-example` project and loopback ports. Choose a
fresh project name for each destructive acceptance/recovery drill. This is a
local execution of the deployable images/configuration, **not** an AWS test.

```sh
uv run python scripts/init_env.py
uv run python scripts/deployment/release.py build --project pulseforge-p7-example
uv run python scripts/deployment/release.py verify --project pulseforge-p7-example
export PULSEFORGE_RELEASE_SHA="$(git rev-parse HEAD)"
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml config --quiet
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml up -d --no-build --wait --wait-timeout 240 postgres kafka minio bootstrap
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml run --rm product-migrate
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml run --rm assistant-migrate
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml --profile streaming up -d --no-build --wait --wait-timeout 240 streaming
```

On PowerShell use `$env:PULSEFORGE_RELEASE_SHA=(git rev-parse HEAD)` instead
of `export`. `release.py` refuses a dirty tree, a default project name,
non-loopback host ports, multiple singleton replicas or enabled provider.
Its manifest records the source SHA and local image IDs but is not a signed
attestation and is not uploaded. The `postgres`, `kafka`, `minio`, `redis`
images in the release override use reviewed digest references. The source
images are built locally and tagged with the SHA; no registry push occurs.

For a bounded traffic sample, run the finite producer explicitly (do not
leave a generator running unattended):

```sh
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml run --rm -e ANOMALY_RATE=0 producer python -m pulseforge.producer --count 50
```

Wait for streaming readiness with Compose's **bounded** `--wait-timeout`,
then query PostgreSQL and MinIO for actual rows/commits before publishing.
Run the finite pipeline from the Airflow image using a unique build key;
Airflow never starts or stops Spark:

```sh
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml run --rm --no-deps airflow python -m pulseforge.product.cli pipeline --build-key local-p7-example --project-dir /opt/pulseforge/analytics --profiles-dir /opt/pulseforge/analytics
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml --profile product up -d --no-build --wait --wait-timeout 180 redis api dashboard
```

Open `http://127.0.0.1:15173` on the same host; the API is loopback
`18000`. Expect explicit unavailable/empty/stale states until data and a
successful build exist. Do not interpret `/health` as data freshness. The
assistant initially uses labeled lexical fallback. To install its pinned
local model and ingest the versioned corpus, explicitly run:

```sh
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml run --rm assistant-model
docker compose -p pulseforge-p7-example -f docker-compose.yml -f infra/docker/compose.release.yml run --rm assistant-ingest
```

These are public dependency/model downloads, not provider inference. Run them
only where public downloads are permitted; model cache is a named volume.
`ASSISTANT_PROVIDER=disabled` remains mandatory for this release. Optional
observability is a diagnostic local profile, not part of critical readiness;
Tempo still runs as root and is not a hardened cloud target. The default
`docker compose up -d --build --wait` for development is unchanged.

For a stop that retains all data, use the same two Compose files/project and
`stop`, **not** `down -v`. For recovery use the [quiesced procedure](recovery.md).
Docker's named volumes and container logs can grow; monitor disk occupancy
and retain backups before capacity changes. One-host failure loses availability
until the host/volume is recovered. Do not claim HA or an RPO/RTO from a
successful local startup.
