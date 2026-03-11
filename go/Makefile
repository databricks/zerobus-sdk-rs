# Makefile for zerobus-sdk-go

.PHONY: help build build-rust build-go clean fmt fmt-go fmt-rust lint lint-go lint-rust check test test-go test-rust examples release

help:
	@echo "Available targets:"
	@echo "  make build          - Build both Rust FFI and Go SDK"
	@echo "  make build-rust     - Build only the Rust FFI layer"
	@echo "  make build-go       - Build only the Go SDK"
	@echo "  make clean          - Remove build artifacts"
	@echo "  make fmt            - Format all code (Go and Rust)"
	@echo "  make fmt-go         - Format Go code"
	@echo "  make fmt-rust       - Format Rust code"
	@echo "  make lint           - Run linters on all code"
	@echo "  make lint-go        - Run Go linters"
	@echo "  make lint-rust      - Run Rust linters"
	@echo "  make check          - Run all checks (fmt and lint)"
	@echo "  make test           - Run all tests (Rust and Go)"
	@echo "  make test-rust      - Run Rust unit tests"
	@echo "  make test-go        - Run Go unit tests"
	@echo "  make examples       - Build all examples"
	@echo "  make release        - Build release package"

build: build-rust build-go

build-rust:
	@echo "Building Rust FFI layer..."
	@ZEROBUS_BUILD_RUST=1
	@# Detect OS and Arch for the target directory
	@OS=$$(uname -s | tr '[:upper:]' '[:lower:]'); \
	ARCH=$$(uname -m); \
	case "$$OS" in \
		darwin*)  GOOS="darwin" ;; \
		linux*)   GOOS="linux" ;; \
		msys*|mingw*|cygwin*) GOOS="windows" ;; \
		*)        GOOS="$$OS" ;; \
	esac; \
	case "$$ARCH" in \
		x86_64)   GOARCH="amd64" ;; \
		aarch64|arm64)  GOARCH="arm64" ;; \
		*)        GOARCH="$$ARCH" ;; \
	esac; \
	LIB_DIR="lib/$${GOOS}_$${GOARCH}"; \
	echo "Target directory: $$LIB_DIR"; \
	mkdir -p $$LIB_DIR; \
	if [ "$$OS" = "Windows_NT" ] || [[ "$$OS" == *"mingw"* ]] || [[ "$$OS" == *"msys"* ]]; then \
		echo "Detected Windows - building for x86_64-pc-windows-gnu target"; \
		cd ../rust/ffi && cargo build --release --target x86_64-pc-windows-gnu; \
		cd -; \
		if [ -f ../rust/target/x86_64-pc-windows-gnu/release/libzerobus_ffi.a ]; then \
			cp ../rust/target/x86_64-pc-windows-gnu/release/libzerobus_ffi.a $$LIB_DIR/; \
		elif [ -f ../rust/target/release/zerobus_ffi.lib ]; then \
			cp ../rust/target/release/zerobus_ffi.lib $$LIB_DIR/libzerobus_ffi.a; \
		fi; \
	else \
		cd ../rust/ffi && cargo build --release; \
		cd -; \
		cp ../rust/target/release/libzerobus_ffi.a $$LIB_DIR/; \
	fi
	@echo "✓ Rust FFI layer built successfully"

build-go: build-rust
	@echo "Building Go SDK..."
	go build -v
	@echo "✓ Go SDK built successfully"

clean:
	@echo "Cleaning build artifacts..."
	cd ../rust/ffi && cargo clean
	rm -rf lib/
	rm -f libzerobus_ffi.a
	rm -rf releases
	@echo "✓ Clean complete"

fmt: fmt-go fmt-rust

fmt-go:
	@echo "Formatting Go code..."
	go fmt ./...
	cd examples/json/single && go fmt ./...
	cd examples/json/batch && go fmt ./...
	cd examples/proto/single && go fmt ./...
	cd examples/proto/batch && go fmt ./...
	cd examples/arrow && go fmt ./...

fmt-rust:
	@echo "Formatting Rust code..."
	cd ../rust/ffi && cargo fmt --all

lint: lint-go lint-rust

lint-go:
	@echo "Linting Go code..."
	go vet ./...
	cd examples/json/single && go vet ./...
	cd examples/json/batch && go vet ./...
	cd examples/proto/single && go vet ./...
	cd examples/proto/batch && go vet ./...
	cd examples/arrow && go vet ./...

lint-rust:
	@echo "Linting Rust code..."
	cd ../rust/ffi && cargo clippy --all -- -D warnings

check: fmt lint

test: test-rust test-go

test-rust:
	@echo "Running Rust tests..."
	cd ../rust/ffi && cargo test -- --test-threads=1

test-go:
	@echo "Running Go unit tests..."
	go test -v -timeout 60s ./...
	cd tests && go test -v -timeout 60s ./...
	@echo "✓ All Go tests passed"

examples: build
	@echo "Building examples..."
	cd examples/json/single && go build -o json-single main.go
	cd examples/json/batch && go build -o json-batch main.go
	cd examples/proto/single && go build -o proto-single main.go
	cd examples/proto/batch && go build -o proto-batch main.go
	cd examples/arrow && go build -o arrow main.go
	@echo "✓ Examples built successfully"

release:
	@echo "Building release package..."
	./scripts/build-release.sh
