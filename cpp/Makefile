# Makefile for the Zerobus C++ SDK.
#
# Thin convenience wrappers around CMake/CTest and clang-format, mirroring the
# build/test/lint/fmt verbs used by the other SDKs in this monorepo.

BUILD_DIR ?= build
BUILD_TYPE ?= Release
JOBS ?= 4

# All hand-written C++ sources (excludes the CMake build tree and fetched deps).
SOURCES := $(shell find include src tests examples -name '*.cpp' -o -name '*.hpp' 2>/dev/null)

.PHONY: help configure build build-ffi test lint fmt fmt-check clean examples check

help:
	@echo "Available targets:"
	@echo "  make build      - Configure and build the SDK, tests, and examples"
	@echo "  make build-ffi  - Build only the underlying Rust C FFI library"
	@echo "  make test       - Build and run the test suite (ctest)"
	@echo "  make examples   - Build the example binaries"
	@echo "  make fmt        - Format all C++ sources with clang-format"
	@echo "  make fmt-check  - Check formatting without modifying files"
	@echo "  make lint       - Run lint checks (formatting + compiler warnings)"
	@echo "  make check      - Run fmt-check and lint"
	@echo "  make clean      - Remove the build directory"

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON

build: configure
	cmake --build $(BUILD_DIR) -j $(JOBS)

# Build just the Rust FFI static library (cargo build --release in rust/ffi).
build-ffi:
	cd ../rust/ffi && cargo build --release

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

examples: build
	@echo "Examples built under $(BUILD_DIR)/examples/"

fmt:
	clang-format -i $(SOURCES)

fmt-check:
	clang-format --dry-run --Werror $(SOURCES)

# Without clang-tidy available everywhere, lint = formatting + the compiler's
# own -Wall -Wextra (enabled in CMakeLists for the library target).
lint: fmt-check build

check: fmt-check lint

clean:
	rm -rf $(BUILD_DIR)
