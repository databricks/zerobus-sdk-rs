#!/usr/bin/env bash
# Point npm at the internal JFrog npm registry.
# Reads JFROG_ACCESS_TOKEN from the environment.
set -euo pipefail

cat > ~/.npmrc << EOF
registry=https://databricks.jfrog.io/artifactory/api/npm/db-npm/
//databricks.jfrog.io/artifactory/api/npm/db-npm/:_authToken=${JFROG_ACCESS_TOKEN}
always-auth=true
EOF
echo "npm configured to use JFrog registry"
