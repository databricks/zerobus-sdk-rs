.PHONY: all build test clean pack restore fmt check restore-native native help

# Default target
all: restore build test

# Restore NuGet packages (use local NuGet.Config to avoid external feeds)
restore:
	dotnet restore --configfile NuGet.Config

# Build all projects
build:
	dotnet build

# Run all tests
test:
	dotnet test

# Run only unit tests (skip integration)
test-unit:
	dotnet test --filter "Category!=Integration"

# Run only integration tests (requires native library)
test-integration:
	dotnet test --filter "Category=Integration"

# Clean build outputs
clean:
	dotnet clean

# Create NuGet package
pack:
	dotnet pack src/Databricks.Zerobus -c Release -o artifacts

# Format code
fmt:
	dotnet format

# Check formatting (CI)
fmt-check:
	dotnet format --verify-no-changes

# Restore + build native (requires Rust toolchain)
native:
	./build_native.sh

# Cross-compile native for all platforms
native-all:
	./build_native.sh --all

# Full release build (native + managed + pack)
release: native build test pack

help:
	@echo "Zerobus .NET SDK — Makefile targets"
	@echo ""
	@echo "  all              Restore, build, and test (default)"
	@echo "  restore          Restore NuGet packages"
	@echo "  build            Build all projects"
	@echo "  test             Run all tests"
	@echo "  test-unit        Run unit tests only"
	@echo "  test-integration Run integration tests (requires native lib)"
	@echo "  clean            Clean build outputs"
	@echo "  pack             Create NuGet package"
	@echo "  fmt              Auto-format code"
	@echo "  fmt-check        Check formatting (CI)"
	@echo "  native           Build native FFI for current platform"
	@echo "  native-all       Cross-compile native for all platforms"
	@echo "  release          Full release: native + build + test + pack"
