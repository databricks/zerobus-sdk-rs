# Makefile for zerobus-sdk-rs

.PHONY: help build build-release clean fmt lint check test build-jni build-jni-test build-ffi

help:
	@echo "Available targets:"
	@echo "  make build          - Build the project for debugging"
	@echo "  make build-release  - Build the project for release"
	@echo "  make build-jni      - Build JNI library for Java SDK"
	@echo "  make build-ffi      - Build C FFI library for language bindings"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make fmt            - Format code with rustfmt"
	@echo "  make lint           - Run linting with clippy"
	@echo "  make check          - Run all checks (fmt and lint)"
	@echo "  make test           - Run tests for all workspace packages"

build:
	cargo build --workspace

build-release:
	cargo build --release --workspace

build-jni:
	cargo build --release -p zerobus-jni

build-jni-test:
	cargo build --release -p zerobus-jni --features test-helpers

build-ffi:
	cargo build --release -p zerobus-ffi

clean:
	cargo clean

fmt:
	cargo fmt --all

lint:
	cargo clippy --all -- -D warnings

check: fmt lint

test:
	cargo test --workspace
