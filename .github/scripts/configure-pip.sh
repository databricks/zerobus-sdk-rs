#!/usr/bin/env bash
# Point pip at the internal JFrog PyPI registry.
# Reads JFROG_ACCESS_TOKEN from the environment and writes PIP_INDEX_URL to $GITHUB_ENV.
set -euo pipefail

echo "PIP_INDEX_URL=https://gha-service-account:${JFROG_ACCESS_TOKEN}@databricks.jfrog.io/artifactory/api/pypi/db-pypi/simple" >> "$GITHUB_ENV"
echo "pip configured to use JFrog registry"
