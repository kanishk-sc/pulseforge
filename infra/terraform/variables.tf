variable "aws_region" {
  description = "Region for the private single-node deployment; review regional prices and quotas."
  type        = string
  default     = "us-east-1"
}

variable "project_name" {
  type    = string
  default = "pulseforge"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,18}$", var.project_name))
    error_message = "Use a short lowercase project name."
  }
}

variable "environment" {
  type    = string
  default = "demo"
  validation {
    condition     = can(regex("^[a-z][a-z0-9-]{1,12}$", var.environment))
    error_message = "Use a short lowercase environment name."
  }
}

variable "availability_zone" {
  description = "One AZ for the private host and NAT gateway. This is not HA."
  type        = string
  validation {
    condition     = startswith(var.availability_zone, var.aws_region)
    error_message = "availability_zone must belong to aws_region."
  }
}

variable "ami_id" {
  description = "Explicit x86_64 Amazon Linux 2023 AMI reviewed for the account/region."
  type        = string
  validation {
    condition     = can(regex("^ami-[0-9a-f]{8,17}$", var.ami_id))
    error_message = "Supply an explicit AMI ID, not a mutable latest image."
  }
}

variable "instance_type" {
  description = "One x86_64 host for the full Compose stack."
  type        = string
  default     = "m6i.2xlarge"
  validation {
    condition     = contains(["m6i.2xlarge", "m6i.4xlarge"], var.instance_type)
    error_message = "Supported x86_64 sizes are m6i.2xlarge and m6i.4xlarge."
  }
}

variable "vpc_cidr" {
  type    = string
  default = "10.42.0.0/16"
}

variable "public_subnet_cidr" {
  type    = string
  default = "10.42.1.0/24"
}

variable "private_subnet_cidr" {
  type    = string
  default = "10.42.2.0/24"
}

variable "data_volume_size_gib" {
  description = "Encrypted gp3 volume for Docker images and named volumes."
  type        = number
  default     = 200
  validation {
    condition     = var.data_volume_size_gib >= 100 && var.data_volume_size_gib <= 2048
    error_message = "Data volume must be between 100 and 2048 GiB."
  }
}

variable "backup_bucket_name" {
  description = "Globally unique operator-chosen backup bucket; no backups are uploaded by Terraform."
  type        = string
  validation {
    condition     = can(regex("^[a-z0-9][a-z0-9.-]{3,61}[a-z0-9]$", var.backup_bucket_name))
    error_message = "Provide a globally unique lowercase S3-compatible bucket name."
  }
}
