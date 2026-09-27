#!/bin/bash
set -euo pipefail
# No credentials, repository checkout, application service or volume formatting here.
# The operator verifies and mounts the protected data volume before Docker starts.
dnf install -y docker git awscli curl python3.12
mkdir -p /usr/local/lib/docker/cli-plugins
curl --fail --show-error --location --retry 3 \
  https://github.com/docker/compose/releases/download/v5.1.4/docker-compose-linux-x86_64 \
  --output /usr/local/lib/docker/cli-plugins/docker-compose
echo '33b208d7e76639db742fae84b966cc01dacae58ca3fc4dabbc907045aefdf0c4  /usr/local/lib/docker/cli-plugins/docker-compose' | sha256sum -c -
chmod 0755 /usr/local/lib/docker/cli-plugins/docker-compose
docker compose version
systemctl enable docker
systemctl enable --now amazon-ssm-agent
