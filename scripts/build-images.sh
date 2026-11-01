#!/usr/bin/env bash
# Builds all service images. Unit tests run during each build.
set -euo pipefail
cd "$(dirname "$0")/.."

for module in payment-service fraud-service notification-service audit-service; do
  echo "=== Building $module"
  DOCKER_BUILDKIT=1 docker build --build-arg MODULE="$module" -t "payments/$module:0.1.0" .
done
