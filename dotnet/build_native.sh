#!/usr/bin/env bash
# build_native.sh — Compile the Zerobus Rust FFI library and copy it to the
# .NET runtimes directory for local development.
#
# Usage:
#   ./build_native.sh              # Build for current platform only
#   ./build_native.sh --all        # Cross-compile for all supported platforms
#   ./build_native.sh --release    # Build in release mode (default)
#   ./build_native.sh --debug      # Build in debug mode
#
# Prerequisites:
#   - Rust toolchain (rustup, cargo)
#   - For cross-compilation: zig (via setup-zig or cargo-zigbuild)
#   - This script must be run from the monorepo root (zerobus-sdk/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
MONOREPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
FFI_CRATE="$MONOREPO_ROOT/rust/ffi"
RUNTIMES_DIR="$SCRIPT_DIR/src/Databricks.Zerobus/runtimes"

BUILD_MODE="--release"
TARGET=""
RUST_DIR="$MONOREPO_ROOT/rust"

# ─── Parse args ────────────────────────────────────────────────────
ALL_PLATFORMS=false
while [[ $# -gt 0 ]]; do
    case "$1" in
        --all) ALL_PLATFORMS=true; shift ;;
        --release) BUILD_MODE="--release"; shift ;;
        --debug) BUILD_MODE=""; shift ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# ─── Detect platform ───────────────────────────────────────────────
detect_target() {
    case "$(uname -s)" in
        Linux)
            case "$(uname -m)" in
                x86_64)  echo "x86_64-unknown-linux-gnu" ;;
                aarch64) echo "aarch64-unknown-linux-gnu" ;;
            esac ;;
        Darwin)
            case "$(uname -m)" in
                x86_64) echo "x86_64-apple-darwin" ;;
                arm64)  echo "aarch64-apple-darwin" ;;
            esac ;;
        MINGW*|MSYS*|CYGWIN*)
            echo "x86_64-pc-windows-msvc" ;;
    esac
}

# ─── Map Rust target → NuGet RID + native file name ─────────────────
map_artifact() {
    case "$1" in
        x86_64-unknown-linux-gnu)     echo "linux-x64:libzerobus_ffi.so" ;;
        aarch64-unknown-linux-gnu)    echo "linux-arm64:libzerobus_ffi.so" ;;
        x86_64-apple-darwin)          echo "osx-x64:libzerobus_ffi.dylib" ;;
        aarch64-apple-darwin)         echo "osx-arm64:libzerobus_ffi.dylib" ;;
        x86_64-pc-windows-msvc)       echo "win-x64:zerobus_ffi.dll" ;;
    esac
}

ALL_TARGETS=(
    "x86_64-unknown-linux-gnu"
    "aarch64-unknown-linux-gnu"
    "x86_64-apple-darwin"
    "aarch64-apple-darwin"
    "x86_64-pc-windows-msvc"
)

# ─── Build single target ───────────────────────────────────────────
build_target() {
    local target="$1"
    local mapping
    mapping="$(map_artifact "$target")"
    IFS=':' read -r rid libname <<< "$mapping"

    echo "  → Building for $target ($rid)..."

    cd "$RUST_DIR"
    cargo build $BUILD_MODE -p zerobus-ffi --target "$target"

    local src
    src="target/$target/release/$libname"
    if [[ -z "$BUILD_MODE" ]]; then
        src="target/$target/debug/$libname"
    fi

    local dest="$RUNTIMES_DIR/$rid/native/$libname"
    mkdir -p "$(dirname "$dest")"
    cp -v "$src" "$dest"
    echo "  ✔ $rid done"
}

# ─── Main ──────────────────────────────────────────────────────────
echo "==> Building Zerobus FFI native libraries"
echo "    FFI crate: $FFI_CRATE"
echo "    Runtimes:  $RUNTIMES_DIR"
echo ""

if $ALL_PLATFORMS; then
    echo "==> Cross-compiling for all $(( ${#ALL_TARGETS[@]} )) platforms..."
    for target in "${ALL_TARGETS[@]}"; do
        build_target "$target"
    done
else
    TARGET="$(detect_target)"
    if [[ -z "$TARGET" ]]; then
        echo "ERROR: Could not detect current platform. Use --all to cross-compile."
        exit 1
    fi
    echo "==> Building for current platform: $TARGET"
    build_target "$TARGET"
fi

echo ""
echo "==> Done. Native libraries placed in:"
find "$RUNTIMES_DIR" -type f -exec ls -lh {} \;
