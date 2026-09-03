#!/bin/bash
set -e

# ─────────────────────────────────────────────────────────────────────────────
# build_native.sh — Build the zerobus_ffi shared library for the .NET SDK
#
# This script builds the Rust FFI crate as a cdylib (shared library) and places
# the output in the dotnet/src/Zerobus/runtimes/<RID>/native/ directory, which
# is the standard .NET layout for native P/Invoke libraries.
#
# Usage:
#   ./build_native.sh            # Build for the current platform
#   ./build_native.sh --force    # Rebuild even if up to date
#   ./build_native.sh --all      # Build all RIDs for the current OS
#
# The script is also invoked automatically by the .NET build (via MSBuild
# targets in Zerobus.csproj) so that `dotnet build` and `dotnet test` always
# have the native library available.
# ─────────────────────────────────────────────────────────────────────────────

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$SCRIPT_DIR/.."
FFI_DIR="$REPO_ROOT/rust/ffi"
RUNTIMES_DIR="$SCRIPT_DIR/src/Zerobus/runtimes"

FORCE=0
BUILD_ALL=0
for arg in "$@"; do
    case "$arg" in
        --force) FORCE=1 ;;
        --all)   BUILD_ALL=1 ;;
        *)
            echo "✗ Unknown argument: $arg"
            exit 1
            ;;
    esac
done

# ── Detect platform ──────────────────────────────────────────────────────────

OS="$(uname -s)"
ARCH="$(uname -m)"

case "$OS" in
    Darwin)
        LIB_NAME="libzerobus_ffi.dylib"
        if [[ $BUILD_ALL -eq 1 ]]; then
            RIDS=("osx-arm64" "osx-x64")
            RUST_TARGETS=("aarch64-apple-darwin" "x86_64-apple-darwin")
        else
            case "$ARCH" in
                arm64)   RIDS=("osx-arm64"); RUST_TARGETS=("aarch64-apple-darwin") ;;
                x86_64)  RIDS=("osx-x64");   RUST_TARGETS=("x86_64-apple-darwin") ;;
                *)       echo "✗ Unsupported macOS architecture: $ARCH"; exit 1 ;;
            esac
        fi
        ;;
    Linux)
        LIB_NAME="libzerobus_ffi.so"
        if [[ $BUILD_ALL -eq 1 ]]; then
            RIDS=("linux-arm64" "linux-x64")
            RUST_TARGETS=("aarch64-unknown-linux-gnu" "x86_64-unknown-linux-gnu")
        else
            case "$ARCH" in
                aarch64|arm64) RIDS=("linux-arm64"); RUST_TARGETS=("aarch64-unknown-linux-gnu") ;;
                x86_64)        RIDS=("linux-x64");   RUST_TARGETS=("x86_64-unknown-linux-gnu") ;;
                *)             echo "✗ Unsupported Linux architecture: $ARCH"; exit 1 ;;
            esac
        fi
        ;;
    MINGW*|MSYS*|CYGWIN*)
        if [[ $BUILD_ALL -eq 1 ]]; then
            echo "✗ --all is not supported on Windows"
            exit 1
        fi
        RIDS=("win-x64")
        RUST_TARGETS=("x86_64-pc-windows-gnu")
        LIB_NAME="zerobus_ffi.dll"
        ;;
    *)
        echo "✗ Unsupported OS: $OS"
        exit 1
        ;;
esac

echo "┌──────────────────────────────────────────────────────────────────┐"
echo "│  Zerobus .NET — Native Library Build                            │"
echo "├──────────────────────────────────────────────────────────────────┤"
echo "│  Platform:  $OS / $ARCH"
if [[ ${#RIDS[@]} -eq 1 ]]; then
    echo "│  RID:       ${RIDS[0]}"
else
    echo "│  RIDs:      ${RIDS[*]}"
fi
echo "│  Library:   $LIB_NAME"
echo "│  Output:    $RUNTIMES_DIR/<RID>/native/"
echo "└──────────────────────────────────────────────────────────────────┘"

# ── Check for Rust toolchain ─────────────────────────────────────────────────

if ! command -v cargo &>/dev/null; then
    # Try sourcing the standard cargo env
    if [[ -f "$HOME/.cargo/env" ]]; then
        # shellcheck disable=SC1091
        source "$HOME/.cargo/env"
    fi
fi

if ! command -v cargo &>/dev/null; then
    echo ""
    echo "✗ cargo not found. Install the Rust toolchain:"
    echo "    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh"
    exit 1
fi

# ── Check if rebuild is needed ───────────────────────────────────────────────

if [[ $FORCE -eq 0 ]]; then
    ALL_UP_TO_DATE=1
    for rid in "${RIDS[@]}"; do
        target_dir="$RUNTIMES_DIR/$rid/native"
        target_path="$target_dir/$LIB_NAME"

        if [[ ! -f "$target_path" ]]; then
            ALL_UP_TO_DATE=0
            break
        fi

        if [[ "$FFI_DIR/Cargo.toml" -nt "$target_path" ]]; then
            ALL_UP_TO_DATE=0
            break
        fi

        while IFS= read -r -d '' file; do
            if [[ "$file" -nt "$target_path" ]]; then
                ALL_UP_TO_DATE=0
                break 2
            fi
        done < <(find "$FFI_DIR/src" -name "*.rs" -print0 2>/dev/null)
    done

    if [[ $ALL_UP_TO_DATE -eq 1 ]]; then
        echo ""
        echo "✓ Native libraries are up to date."
        echo "  (use --force to rebuild anyway)"
        exit 0
    fi
fi

# ── Build ────────────────────────────────────────────────────────────────────

echo ""
echo "Building zerobus-ffi (release)..."
echo ""

cd "$FFI_DIR"

build_target() {
    rid="$1"
    rust_target="$2"
    target_dir="$RUNTIMES_DIR/$rid/native"
    target_path="$target_dir/$LIB_NAME"

    echo "→ Building $rid ($rust_target)"

    # Build with optional features based on ZEROBUS_BUILD_FEATURES env var
    if [[ -n "$ZEROBUS_BUILD_FEATURES" ]]; then
        cargo build --release --features "$ZEROBUS_BUILD_FEATURES" --target "$rust_target"
    else
        cargo build --release --target "$rust_target"
    fi

    cargo_out="../target/$rust_target/release/$LIB_NAME"

    if [[ ! -f "$cargo_out" ]]; then
        echo ""
        echo "✗ Build succeeded but library not found at: $cargo_out"
        echo "  Expected shared library ($LIB_NAME) — check Cargo.toml has:"
        echo '    crate-type = ["staticlib", "cdylib"]'
        exit 1
    fi

    mkdir -p "$target_dir"
    cp "$cargo_out" "$target_path"
}

for i in "${!RIDS[@]}"; do
    build_target "${RIDS[$i]}" "${RUST_TARGETS[$i]}"
done

# ── Copy to runtimes ─────────────────────────────────────────────────────────

echo ""
echo "✓ Native libraries built and placed under:"
echo "    $RUNTIMES_DIR/<RID>/native/"
echo ""
