# Release order, failure isolation and rollback

Use one clean, reviewed source commit and its `phase7-release.json` image-ID
manifest. The `scripts/deployment/release.py` tool checks image architecture,
SHA, complete image IDs and private/singleton Compose configuration. Run its
`verify` command after `build` and before starting a release. Locally built
services cannot pull a missing tag from a registry. The tool never pushes or
deploys to AWS. The cloud host is not initialized by Terraform user data. A future
authorized operator must first verify the AMI/SSM session, protected EBS
volume identity/mount, Docker data-root, private `.env`, disk space, approved
source/images, and a recoverable quiesced backup. Do not put application
passwords, state, archives or local model cache in Git or image layers.

Order for a new isolated project:

1. Bring up PostgreSQL, Kafka and MinIO with bounded Compose readiness. Rerun
   `bootstrap` (topics/buckets). Failure leaves the data containers available
   for diagnosis; do not start producer/Spark until Kafka/MinIO are healthy.
2. Run product and assistant schema migrations as finite jobs. Their packaged
   SQL versions are recorded and re-runs return no new versions. Advisory
   transaction locks reject concurrent migration attempts. These migrations
   do not truncate event, ledger or corpus data. PostgreSQL's pgvector comes
   from the exact pgvector image; never substitute stock PostgreSQL silently.
3. Start **one** Spark stream independently. The MinIO checkpoint namespace
   belongs to that stream only. Check live query health, PostgreSQL source
   counts and committed lake metadata. A health probe is not freshness proof.
4. Run one finite dbt/publication/detector pipeline with a unique build key.
   The pipeline holds a session advisory lock to block overlapping manual or
   Airflow pipelines. Direct `analytics-dbt` jobs bypass that lock and must
   never overlap pipeline runs. Publication becomes visible only after a full
   successful dbt build; failed builds leave the last success in place.
5. Start Redis, API and dashboard; check `/health`, `/ready`, publication status,
   a bounded metric request and real browser states. Run finite assistant
   model download/migration/ingest explicitly if local embeddings are desired.
   Start optional observability last. Airflow may schedule finite jobs only;
   it never controls Spark. Do not scale singleton services horizontally.

Use bounded `docker compose ... --wait --wait-timeout` and the existing
readiness checks, not unbounded sleeps. On any failed stage, stop **new**
dependent work; leave persistent services and volumes intact for diagnosis.
Ingestion ledgers, source uniqueness and build keys make safe repetitions
possible, but check the failed step's logs/state before retrying. Do not
re-run an unreviewed dbt schema change over a successful published generation.

## Application rollback versus data rollback

An application-only rollback can redeploy the previous **retained image IDs**
and source SHA on the same data volumes only when SQL schema, event contract,
Kafka format, MinIO lake format and Spark checkpoint compatibility have been
reviewed both ways. Stop producer/stream/finite jobs, check the old manifest,
start infrastructure and old images in the order above, then verify readiness,
publication and replay. A mutable tag rebuild is not a reliable rollback.

There is no automatic database migration rollback. For an incompatible schema,
checkpoint or Kafka change, stop all writers and restore a coherent quiesced
backup into **new** project volumes using [recovery](recovery.md), then inspect
loss between backup and failure before cutover. A PostgreSQL-only restore
against newer MinIO commits/checkpoints is unsupported. A full rollback can
lose post-backup events and requires an explicit operator decision, not a
silent `docker compose down -v`. Keep old data until the separate restore is
verified. No RPO/RTO guarantee or multi-host failover is implied.
