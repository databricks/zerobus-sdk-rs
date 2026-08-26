#!/bin/bash

# Generate Proto Tool - Helper script for running GenerateProto from the SDK JAR
#
# This script runs the GenerateProto tool to generate proto2 files
# from Unity Catalog table schemas.
#
# The tool is packaged within the Zerobus SDK JAR and can be executed
# directly without needing to clone the repository.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="${SCRIPT_DIR}/.."
TARGET_DIR="${PROJECT_ROOT}/target"

# Find the shaded JAR (with dependencies)
SHADED_JAR=$(find "${TARGET_DIR}" -name "*zerobus-ingest-sdk-*-jar-with-dependencies.jar" 2>/dev/null | head -n 1)

if [ -z "${SHADED_JAR}" ] || [ ! -f "${SHADED_JAR}" ]; then
    echo "Error: Zerobus SDK JAR not found in ${TARGET_DIR}"
    echo "Please run 'mvn package -Dzerobus.skipNativeLibCheck=true' first to build the SDK JAR"
    exit 1
fi

# Run the tool from the JAR
java -cp "${SHADED_JAR}" com.databricks.zerobus.tools.GenerateProto "$@"
