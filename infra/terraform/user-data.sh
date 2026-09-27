#!/bin/bash
set -euo pipefail
# No credentials, repository checkout, application service or volume formatting here.
# The operator verifies and mounts the protected data volume before Docker starts.
dnf install -y docker git awscli
systemctl enable docker
systemctl enable --now amazon-ssm-agent
