#!/usr/bin/env bash
set -euo pipefail

readonly RUSTFMT_TOOLCHAIN="nightly-2025-08-07"

exec cargo +"${RUSTFMT_TOOLCHAIN}" fmt "$@"
