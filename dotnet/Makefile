# Makefile for zerobus-sdk-dotnet

.PHONY: help build build-rust build-dotnet clean fmt fmt-dotnet fmt-rust lint lint-dotnet lint-rust check test test-dotnet test-rust examples release

help:
	@echo "Available targets:"
	@echo "  make build          - Build both Rust FFI and .NET SDK"
	@echo "  make build-rust     - Build only the Rust FFI layer"
	@echo "  make build-dotnet   - Build only the .NET SDK"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make fmt            - Format all code (Rust and .NET)"
	@echo "  make fmt-dotnet     - Format .NET code"
	@echo "  make fmt-rust       - Format Rust code"
	@echo "  make lint           - Run linters on all code"
	@echo "  make lint-dotnet    - Run .NET linters"
	@echo "  make lint-rust      - Run Rust linters"
	@echo "  make check          - Run all checks (fmt and lint)"
	@echo "  make test           - Run all tests (Rust and .NET)"
	@echo "  make test-rust      - Run Rust unit tests"
	@echo "  make test-dotnet    - Run .NET unit tests"
	@echo "  make examples       - Build all examples"

build: build-rust build-dotnet

build-rust:
	./build_native.sh
build-rust-force:
	./build_native.sh --force

build-dotnet: build-rust
	@echo "Building .NET SDK..."
	dotnet build
	@echo "✓ .NET SDK built successfully"

clean:
	@echo "Cleaning build artifacts..."
	cd ../rust/ffi && cargo clean
	rm -rf src/Zerobus/runtimes/*
	rm -f libzerobus_ffi.a
	rm -rf releases
	@echo "✓ Clean complete"

fmt: fmt-dotnet fmt-rust

fmt-dotnet:
	@echo "Formatting .NET code..."
	dotnet format 

fmt-rust:
	@echo "Formatting Rust code..."
	cd ../rust/ffi && cargo fmt --all

lint: lint-dotnet lint-rust

lint-dotnet:
	@echo "Linting .NET code..."
	@echo ".NET linting not available as standalone yet - skipping lint-dotnet"

lint-rust:
	@echo "Linting Rust code..."
	cd ../rust/ffi && cargo clippy --all -- -D warnings

check: fmt lint

test: test-rust test-dotnet

test-rust:
	@echo "Running Rust tests..."
	cd ../rust/ffi && cargo test -- --test-threads=1

test-dotnet:
	@echo "Running .NET unit tests..."
	dotnet test -p:SkipNativeBuild=true
	@echo "✓ All .NET tests passed"

examples: build
	@echo "Building examples..."
	dotnet build examples/JsonSingle
	dotnet build examples/JsonBatch
	dotnet build examples/ProtoSingle
	@echo "✓ Examples built successfully"
