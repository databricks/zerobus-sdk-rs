# Default Python version (can be overridden: make build PYTHON=python3.11)
# Note: SDK requires Python 3.9+ (maturin 1.5+ requires Python 3.7+)
PYTHON ?= python3

ifeq ($(OS),Windows_NT)
    VENV = .venv/Scripts/python
else
    VENV = .venv/bin/python
endif

.PHONY: dev install build clean clean-all install-wheel help fmt lint test build-rust develop-rust clean-rust lock-dev

help:
	@echo "Available targets:"
	@echo ""
	@echo "Python targets:"
	@echo "  make build          - Build *release* wheel (Rust + Python)"
	@echo "  make install-wheel  - Install the built wheel"
	@echo "  make install        - Install package directly (editable mode)"
	@echo "  make dev            - Set up development environment"
	@echo "  make clean          - Remove build artifacts (keeps .venv)"
	@echo "  make clean-all      - Remove build artifacts and .venv"
	@echo "  make fmt            - Format code"
	@echo "  make lint           - Run linting"
	@echo "  make test           - Run unit tests"
	@echo ""
	@echo "Rust targets:"
	@echo "  make build-rust     - Build Rust extension only (release)"
	@echo "  make develop-rust   - Build and install Rust extension (dev mode)"
	@echo "  make clean-rust     - Remove Rust build artifacts"
	@echo ""
	@echo "Example: make build PYTHON=python3.11"

dev:
	$(PYTHON) -m venv .venv
	$(VENV) -m pip install --upgrade pip
	@test -f LICENSE || cp ../LICENSE LICENSE
	$(VENV) -m pip install -r requirements-dev.lock
	$(VENV) -m maturin develop

lock-dev: # requires uv
	@test -f LICENSE || cp ../LICENSE LICENSE
	uv pip compile --generate-hashes --no-emit-index-url --exclude-newer '2026-03-19T00:00:00Z' --output-file requirements-dev.lock requirements-dev.in

install:
	$(VENV) -m pip install -e .

build:
	@echo "Building release wheel with $(PYTHON)..."
	$(VENV) -m pip install --upgrade pip maturin
	$(VENV) -m maturin build --release --strip --out dist
	@echo ""
	@echo "✓ Release wheel built successfully in dist/"
	@ls -lh dist/*.whl 2>/dev/null || true

install-wheel:
	@if [ -z "$$(ls -t dist/*.whl 2>/dev/null | head -1)" ]; then \
		echo "Error: No wheel found in dist/. Run 'make build' first."; \
		exit 1; \
	fi
	@echo "Installing wheel: $$(ls -t dist/*.whl | head -1)"
	$(VENV) -m pip install --force-reinstall $$(ls -t dist/*.whl | head -1)
	@echo "✓ Wheel installed successfully"

clean:
	rm -rf dist *.egg-info .pytest_cache build htmlcov

clean-all: clean clean-rust
	rm -rf .venv

fmt:
	$(VENV) -m black zerobus examples tests
	$(VENV) -m autoflake -ri --exclude '*_pb2*.py' zerobus examples tests
	$(VENV) -m isort zerobus examples tests
	cd rust && ../../.github/scripts/rustfmt.sh --all

lint:
	$(VENV) -m pycodestyle --exclude='*_pb2*.py' --max-line-length=120 --ignore=E203,W503 zerobus
	$(VENV) -m autoflake --check-diff --quiet --recursive --exclude '*_pb2*.py' zerobus

test:
	$(VENV) -m pytest --cov=zerobus --cov-report html --cov-report xml tests

# Rust targets (v0.3.0+)
build-rust:
	@echo "Building Rust extension (release mode)..."
	@which maturin >/dev/null 2>&1 || (echo "Error: maturin not found. Install with: pip install maturin" && exit 1)
	$(VENV) -m maturin build --release
	@echo "✓ Rust extension built successfully"

develop-rust:
	@echo "Building and installing Rust extension (development mode)..."
	@which maturin >/dev/null 2>&1 || (echo "Error: maturin not found. Install with: pip install maturin" && exit 1)
	$(VENV) -m maturin develop
	@echo "✓ Rust extension installed in development mode"

clean-rust:
	@echo "Cleaning Rust build artifacts..."
	rm -rf target/
	rm -rf rust/target/
	rm -f zerobus/_zerobus_core.*
	rm -f Cargo.lock
	@echo "✓ Rust artifacts cleaned"
