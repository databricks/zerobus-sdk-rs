# Makefile for the Zerobus C++ SDK.
#
# Thin convenience wrappers around CMake/CTest and clang-format, mirroring the
# build/test/lint/fmt verbs used by the other SDKs in this monorepo.

BUILD_TYPE ?= Release
JOBS ?= 4

# Optional runtime sanitizer, e.g. `make test SANITIZE=address` (or thread,
# undefined). Empty by default, so normal builds are unaffected.
SANITIZE ?=
SANITIZE_FLAG := $(if $(SANITIZE),-DZEROBUS_SANITIZE=$(SANITIZE),)

# Sanitizer builds use their own build dir so they don't clash with a normal
# (GCC) build dir — CMake caches the compiler, so reusing one build dir across
# compilers errors. Override BUILD_DIR to force a specific path.
BUILD_DIR ?= $(if $(SANITIZE),build-$(SANITIZE),build)

# address/thread also instrument the Rust FFI (see BuildRustFfi.cmake), which
# emits LLVM sanitizer runtime calls. GCC's sanitizer runtime can't resolve
# those, so build the C++ side with clang to match. Override CLANG_CC/CLANG_CXX
# for a versioned binary (e.g. `make test SANITIZE=thread CLANG_CXX=clang++-18`).
CLANG_CC ?= clang
CLANG_CXX ?= clang++
ifneq ($(filter $(SANITIZE),address thread),)
SANITIZE_FLAG += -DCMAKE_C_COMPILER=$(CLANG_CC) -DCMAKE_CXX_COMPILER=$(CLANG_CXX)
endif

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
	@echo ""
	@echo "  Add SANITIZE=address (or thread/undefined) to build+test with a sanitizer,"
	@echo "  e.g. 'make test SANITIZE=address'."

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) -DCMAKE_EXPORT_COMPILE_COMMANDS=ON $(SANITIZE_FLAG)

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
