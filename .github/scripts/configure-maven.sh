#!/usr/bin/env bash
set -euo pipefail

# Configure Maven to resolve dependencies through the JFrog mirror.
# Writes ~/.m2/settings.xml with mirror and server credentials.
#
# The JFrog db-maven virtual repo proxies Maven Central for project
# dependencies. All artifact resolution is routed through JFrog.
#
# Requires JFROG_ACCESS_TOKEN to be set in the environment.

: "${JFROG_ACCESS_TOKEN:?JFROG_ACCESS_TOKEN is required}"

mkdir -p ~/.m2
cat > ~/.m2/settings.xml << 'SETTINGS_TEMPLATE'
<settings>
  <mirrors>
    <mirror>
      <id>jfrog-central</id>
      <mirrorOf>*</mirrorOf>
      <url>https://databricks.jfrog.io/artifactory/db-maven/</url>
    </mirror>
  </mirrors>
  <servers>
    <server>
      <id>jfrog-central</id>
      <username>gha-service-account</username>
      <password>JFROG_TOKEN_PLACEHOLDER</password>
    </server>
  </servers>
</settings>
SETTINGS_TEMPLATE

# Substitute the token (avoids exposing it in a heredoc that uses shell expansion).
sed -i "s|JFROG_TOKEN_PLACEHOLDER|${JFROG_ACCESS_TOKEN}|" ~/.m2/settings.xml

echo "Maven configured to use JFrog registry"
