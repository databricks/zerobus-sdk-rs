# Makefile for the Zerobus Pure C SDK: convenience wrappers around CMake/CTest.

BUILD_TYPE ?= Release
JOBS ?= 4

SANITIZE ?=
SANITIZE_FLAG := $(if $(SANITIZE),-DZEROBUS_SANITIZE=$(SANITIZE),)

# Extra -D flags forwarded to cmake configure (lint sets -DZEROBUS_WERROR=ON).
CMAKE_EXTRA ?=

# Sanitizer builds use their own build dir so they don't clash with a normal
# build dir. Override BUILD_DIR to force a specific path.
BUILD_DIR ?= $(if $(SANITIZE),build-$(SANITIZE),build)

# All hand-written C sources (excludes the CMake build tree).
SOURCES := $(shell find include src examples tests -name '*.c' -o -name '*.h' 2>/dev/null)

.PHONY: help configure build test examples lint fmt fmt-check clean check verify

help:
	@echo "Available targets:"
	@echo "  make build      - Configure and build the SDK, examples, and tests"
	@echo "  make test       - Build and run the offline test suite (ctest)"
	@echo "  make examples   - Build the example binaries"
	@echo "  make fmt        - Format all C sources with clang-format"
	@echo "  make fmt-check  - Check formatting without modifying files"
	@echo "  make lint       - Warnings-as-errors build (-Werror)"
	@echo "  make check      - fmt-check + lint (static gates, no tests)"
	@echo "  make verify     - Full workspace check: check + tests, plain and sanitized"
	@echo "  make clean      - Remove the build directory"
	@echo ""
	@echo "  Add SANITIZE=address (or thread/undefined) to build+test with a"
	@echo "  sanitizer, e.g. 'make test SANITIZE=address'."

configure:
	cmake -S . -B $(BUILD_DIR) -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) \
		-DCMAKE_EXPORT_COMPILE_COMMANDS=ON $(SANITIZE_FLAG) $(CMAKE_EXTRA)

build: configure
	cmake --build $(BUILD_DIR) -j $(JOBS)

test: build
	cd $(BUILD_DIR) && ctest --output-on-failure

examples: build
	@echo "Examples built under $(BUILD_DIR)/examples/"

fmt:
	clang-format -i $(SOURCES)

fmt-check:
	clang-format --dry-run --Werror $(SOURCES)

# No clang-tidy here, so -Wall -Wextra is the linter: run the build with
# -Werror. Its own tree keeps the flag out of the normal build's cache.
lint:
	$(MAKE) build BUILD_DIR=build-lint CMAKE_EXTRA=-DZEROBUS_WERROR=ON

check: fmt-check lint

# Full workspace check: static checks, then the suite plain and under all three
# sanitizers. address and thread can't coexist, so undefined rides with each.
verify:
	$(MAKE) check
	$(MAKE) test
	$(MAKE) test SANITIZE=address,undefined
	$(MAKE) test SANITIZE=thread,undefined

clean:
	rm -rf build build-*
