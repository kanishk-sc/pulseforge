# AWS boundary, cost worksheet and teardown (not executed)

The Phase 7 module has **never been applied**. It provisions a private single
host, not managed PostgreSQL/Kafka/Redis or public HTTPS. All prices below are
illustrative on-demand assumptions for **us-east-1, reviewed 2026-09-27**;
operators must obtain a current account-specific quote before any apply.
No introductory credit/free-tier assumption is used.

| Always-on item | Quantity / assumption | Illustrative monthly USD (730 h) |
| --- | --- | ---: |
| EC2 `m6i.2xlarge` Linux | One x86_64 8-vCPU/32-GiB host at $0.384/h | $280.32 |
| NAT Gateway | One at $0.045/h | $32.85 |
| Encrypted gp3 EBS | 30-GiB root + 200-GiB protected data at $0.08/GiB-month; baseline performance only | $18.40 |
| NAT public IPv4 | One allocated/used address at $0.005/h | $3.65 |
| S3 Standard backup | Example 50 GB at $0.023/GB-month; versions grow separately | $1.15 |
| NAT processing | Example 20 GB/month at $0.045/GB | $0.90 |
| **Example subtotal** | Excludes internet transfer, requests, extra snapshot/version storage, taxes and operator services | **$337.27/month** |

Rates and sizing sources: [AWS m6i instance specification](https://aws.amazon.com/ec2/instance-types/m6i/),
[AWS EC2 example rate](https://docs.aws.amazon.com/prescriptive-guidance/latest/optimize-costs-microsoft-workloads/consolidate-instances.html),
[AWS VPC NAT/public IPv4 pricing](https://aws.amazon.com/vpc/pricing/),
[AWS EBS gp3 pricing](https://aws.amazon.com/ebs/pricing/), and
[AWS S3 pricing](https://aws.amazon.com/s3/pricing/). Rates can differ by date,
region, purchase option and account. The $337.27 example is neither a capacity
measurement nor a bill forecast. The linked AWS EC2 example uses a Windows
Server workload; its $0.384/h compute figure is an **illustrative assumption**
here, not verification of a current Linux quote. Confirm the Linux on-demand
rate in the intended account before provisioning. Extra EBS IOPS/throughput,
S3 requests and version churn, internet egress, CloudWatch/SSM logging, backup copies,
snapshots, support, taxes, and any model/provider service are **not included**.
No ALB, RDS, MSK, ElastiCache, ECR or Kubernetes charge is modeled because
none is provisioned. A budget alarm is an operator prerequisite, not created.

Stopping Compose lowers active CPU/network use but does **not** stop EC2,
NAT, EBS or S3 billing. Stopping EC2 removes its instance-hour charge but
NAT, EBS, S3, allocated IPv4 and backup versions still charge. A single NAT
gateway and AZ avoid a multi-AZ premium but also create an outage domain.
No auto-teardown exists.

## Security and secret boundary

The instance has no public IP and its security group has no ingress. Private
subnet egress is TCP 443 and VPC DNS via NAT/S3 endpoint. SSM Session Manager
requires IAM and provides an authenticated tunnel to the **loopback** dashboard;
it does not make the application authenticated. Docker bridge traffic between
services, Kafka and MinIO is plaintext on the host. There is no public TLS
termination, WAF, CORS hardening or API rate-limit boundary for external users.
Administrative UIs, API, metrics and OTLP endpoints must not be forwarded to
untrusted clients. The optional provider stays disabled and incident/runbook
data is not sent to a provider by the release flow. MinIO and PostgreSQL
passwords are injected by an operator-owned mode-0600 `.env`; rotate by
coordinated service restarts and verify access before retiring old credentials.
Terraform does not generate/store them. The instance profile permits SSM and
only its backup bucket `backups/` prefix, not general object-store access.
The host and local volume remain a privileged trust boundary: Docker access
is effectively root. Restrict SSM IAM and host operators accordingly.

## Future authorized teardown sequence

1. Stop producer/Spark and then all project containers; make a quiesced backup.
   Verify hashes **and** a separate restore, then upload the archive only with
   separate data-transfer authorization. Keep the source EBS volume until the
   restored PostgreSQL, lake commits/checkpoints and Kafka replay are checked.
2. Export/retain the reviewed Terraform state and record volume ID, S3 bucket
   versions and archive location. Disable termination protection only after
   approval. Review the exact destroy plan; do not blindly remove lifecycle
   guards or run a blanket destroy on old ECS state.
3. Destroy non-retained compute/network resources in an authorized change.
   The protected data volume and backup bucket are intentionally retained by
   `prevent_destroy`; object versions and a remote state bucket are separate
   retention decisions. Inventory them, their recurring charges and named
   owners. Never delete them merely to make Terraform destroy pass.

An account-aware `terraform plan`, AMI/SSM test, IAM validation and actual
bill are future checks requiring separate authorization. The local tests
cannot establish cloud network, host sizing, volume attach or recovery time.
