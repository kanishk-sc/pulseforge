# Phase 7 deployment decision and coverage

Status: local delivery and credential-free AWS configuration in progress;
live AWS deployment requires separate authorization. Design date: 2026-09-27.

## Decision

Use the existing Compose topology as the deployment unit on one private
x86_64 EC2 host with a protected data EBS volume. This preserves one Kafka
broker, a single Spark checkpoint owner, the single PostgreSQL warehouse
writer, MinIO lake commits, finite Airflow/dbt jobs and local assistant assets.
The previous ECS/RDS/Redis/S3/ECR Terraform scaffold described only an API and
dashboard; it could not execute the platform end to end and its public ALB
would have exposed an unauthenticated application. The new Terraform module
replaces, rather than layers onto, that incompatible scaffold. It is a
costed, private **single failure domain**, not an HA or autoscaling claim.

Kubernetes is explicitly deferred. Nothing in this phase creates a Kubernetes
cluster or manifests. A later design would need defined storage ownership,
replication, secrets, and a separate local-cluster acceptance. The roadmap's
live-cloud and autoscaling portions remain pending, not silently checked off.

| Area found at inspection | Status before Phase 7 | Phase 7 disposition |
| --- | --- | --- |
| Default local Compose, pinned MinIO checksum repair, full data path | Implemented and executed in earlier phases | Preserved without changing its default invocation; separate release override and project |
| Python, Spark, dbt, Airflow, dashboard, MinIO images | Built/tested; local mutable tags | Commit-tagged release builds and identity manifest; core service digests pinned in release override |
| Partial ECS/RDS/ElastiCache/S3/ECR Terraform | Implemented, statically validated, never deployed; missing Kafka/Spark/Airflow | Replaced with coherent private single-host target; never migrate old state automatically |
| Kubernetes runtime | Missing | Deferred explicitly; no claims of Kubernetes verification |
| Recovery drill and AWS runtime evidence | Missing | Quiesced isolated-volume drill locally; AWS path remains unexecuted |

## Invariants and boundaries

- The release override binds every host port to `127.0.0.1`; Terraform permits
  no inbound connections. SSM port forwarding requires operator IAM. Neither
  control authenticates individual application users or adds HTTPS to internal
  Docker traffic. Public exposure is forbidden in this design.
- The application runs in one Compose project; Spark, producer, Airflow and all
  finite warehouse jobs have one supported replica. The stream is started
  independently of Airflow. A session advisory lock serializes manual and
  Airflow analytics publication; transactional locks serialize product and
  corpus migrations. Existing sink/ledger uniqueness remains authoritative.
- The release builds source-commit-tagged images and records local image IDs.
  It never pushes a registry. The pinned MinIO asset and SHA-256 check repair
  availability only; they are not a security upgrade. Base build images are
  version-tagged rather than digest-pinned, so byte-for-byte rebuilds across
  registries are not guaranteed; record the manifest and retain image archives
  for a future authorized rollout.
- Model download and corpus indexing are explicit finite setup steps. The
  provider stays disabled by default, no billable inference is required, and
  missing assets give a labeled lexical fallback. Telemetry is optional and
  cannot veto data commits. Tempo's inherited root execution is a known
  local-only exception; the optional profile is not a hardened cloud service.
- A backup is consistent only after **all** project containers are stopped.
  PostgreSQL, Kafka, MinIO, Spark checkpoints, Airflow state and corpus then
  share a quiesced point. Redis/model/telemetry copies are conveniences, not
  business authority. Restore never overwrites an existing project volume.

See [component mapping](../deployment/components.md),
[local operator runbook](../deployment/local-runbook.md),
[release and rollback](../deployment/release-rollback.md),
[recovery](../deployment/recovery.md), and
[AWS cost/security guide](../deployment/aws-cost.md).
