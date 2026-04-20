#!/usr/bin/env bash
# Point cargo at the internal JFrog crates index (sparse registry).
# Expects JFROG_ACCESS_TOKEN in the environment (e.g. from jfrog-oidc-token.sh).
# Appends CARGO_REGISTRIES_JFROG_TOKEN to $GITHUB_ENV when set.
set -euo pipefail

mkdir -p ~/.cargo
cat > ~/.cargo/config.toml << EOF
[source.crates-io]
replace-with = "jfrog"

[source.jfrog]
registry = "sparse+https://databricks.jfrog.io/artifactory/api/cargo/db-cargo-remote/index/"

[registries.jfrog]
index = "sparse+https://databricks.jfrog.io/artifactory/api/cargo/db-cargo-remote/index/"
credential-provider = ["cargo:token"]
EOF

cat > ~/.cargo/credentials.toml << EOF
[registries.jfrog]
token = "Bearer ${JFROG_ACCESS_TOKEN}"
EOF

if [[ -n "${GITHUB_ENV:-}" ]]; then
  echo "CARGO_REGISTRIES_JFROG_TOKEN=Bearer ${JFROG_ACCESS_TOKEN}" >> "$GITHUB_ENV"
fi

echo "Cargo configured to use JFrog registry"
