# Private single-host AWS configuration (not applied)

This module describes **one** private Amazon Linux 2023 x86_64 host running the
version-controlled Compose release, not an ECS or managed-data-service deployment.
It creates one VPC/AZ, a public NAT subnet, a private host subnet, a NAT gateway,
an S3 gateway endpoint, an encrypted EC2 root volume, a protected encrypted gp3
data volume, an SSM instance role, and an encrypted/versioned private backup bucket.
The host security group has **no ingress**, the instance has no public IP, and
there is no ALB, DNS, public API, RDS, ElastiCache, MSK, ECR, or Kubernetes resource.
All application components, including PostgreSQL/pgvector, one KRaft broker,
MinIO, Spark, dbt, Airflow, API, dashboard and Redis, stay in the single Compose
project. This is a non-HA evaluation target, not a production architecture.

No resource has been provisioned by Phase 7. Do not run an AWS plan/apply/destroy
without separate account, region, budget, exposure and resource authorization.
Do not apply this module to state created by the obsolete partial ECS module:
the resource addresses are incompatible and Terraform could propose destructive
replacement. Inventory that state and agree a migration separately.

## Credential-free validation

```sh
terraform fmt -check -recursive
terraform init -backend=false -lockfile=readonly -input=false
terraform validate -no-color
```

The committed AWS provider lock and Terraform 1.10.5 are used in CI. Validation
checks configuration structure only: no account, AMI, quotas, IAM policy,
network path, container startup, pricing or AWS service behavior is verified.
There are no mocked-provider tests and no live plan.

## Future authorized operator prerequisites

1. Confirm the AWS account/region/AZ, an exact x86_64 Amazon Linux 2023 AMI ID,
   quotas, budget alarms, SSM permissions, and a unique backup bucket name. Enter
   those in an **untracked** tfvars file. The application secrets are never
   Terraform variables or generated into its state.
2. Provision a **separate existing** versioned, encrypted, access-controlled S3
   state bucket with restricted operators and, if required, a separate lockfile
   policy. Configure the partial S3 backend in `versions.tf` with a unique key
   and `use_lockfile=true`; never reuse the obsolete ECS state key. State can
   contain sensitive infrastructure metadata. Back it up and restrict access.
3. Review a saved plan and regional costs before an explicitly approved apply.
   Terraform creates infrastructure only. `user-data.sh` installs Docker, Git
   and AWS CLI and enables SSM; it neither formats the protected data volume nor
   checks out source, injects secrets, starts containers, or uploads data.
4. Verify the attached volume by its EBS volume ID, format it **only if proven
   empty and explicitly intended**, mount it, and put Docker's data-root there.
   Stage a reviewed source commit or approved image archive and a root-owned
   mode-0600 application `.env` over an authenticated private operator channel.
   The instance role grants SSM and only `backups/` reads/writes in its bucket;
   it does not provide static AWS keys to containers. The MinIO credentials in
   `.env` are local to this host. Follow the release and recovery runbooks.

The private host needs outbound TLS through the NAT gateway for package/image
downloads and SSM. The S3 gateway endpoint routes backup traffic without NAT
data processing. Port forwarding to the loopback dashboard uses the
`session_manager_dashboard_command` output; an operator's IAM permission is
the access gate, **not** application login. The API, dashboard and all admin
ports are otherwise unauthenticated internally. Do not add public ingress or
forward admin ports to untrusted clients.

`disable_api_termination` protects the instance from ordinary termination;
`prevent_destroy` protects the data volume and backup bucket. These safeguards
intentionally make a blanket `terraform destroy` fail. They are not backups.
The EBS volume, bucket versions, NAT gateway/EIP and storage continue charging
when application containers or the instance are idle/stopped. Teardown needs a
reviewed backup and explicit retained-resource inventory; see the
[AWS and cost guide](../../docs/deployment/aws-cost.md).
