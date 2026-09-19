# AWS deployment target

This Terraform is a validated deployment target, not evidence of a live deployment.
It provisions a small public ALB and ECS/Fargate services for the API and dashboard,
with private-access RDS PostgreSQL and ElastiCache Redis, an encrypted/versioned S3
lake bucket, ECR repositories, CloudWatch logs, and scoped IAM roles.

The public subnets and Fargate public IPs intentionally avoid a NAT Gateway for a
portfolio/demo environment. RDS and Redis have no public endpoints and accept traffic
only from the ECS security group. Before production use, add HTTPS/ACM, WAF, private
ECS subnets with VPC endpoints, Redis authentication, multi-AZ data services, alarms,
and a remote encrypted Terraform backend.

Kafka, Spark and Airflow are not deployed by this module. Select managed or separately
operated data-plane services after measuring workload and cost; the local Compose stack
remains the complete reproducible demonstration.

Validation does not need AWS credentials:

```sh
terraform init -backend=false
terraform fmt -check -recursive
terraform validate
```

Planning or applying creates billable resources and requires AWS credentials plus
immutable `api_image` and `dashboard_image` values. Review the plan and estimated cost
before applying. Deletion protection is enabled for RDS and no automated apply exists.
