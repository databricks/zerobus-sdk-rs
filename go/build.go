package zerobus

//go:generate bash build_rust.sh

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
)

// This file provides utilities for rebuilding the Rust FFI library.
// Tagged releases and checkouts that include go/lib/ archives do not need
// `go generate`. Run it, or set ZEROBUS_BUILD_RUST=1, only when rebuilding
// the FFI from source.

func init() {
	if _, exists := os.LookupEnv("ZEROBUS_BUILD_RUST"); !exists {
		return
	}

	// Check if library exists and provide helpful error if not
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return
	}
	sdkDir := filepath.Dir(filename)
	libPath := filepath.Join(sdkDir, "lib", fmt.Sprintf("%s_%s", runtime.GOOS, runtime.GOARCH), "libzerobus_ffi.a")

	if _, err := os.Stat(libPath); os.IsNotExist(err) {
		fmt.Fprintf(os.Stderr, "\n"+
			"═══════════════════════════════════════════════════════════════\n"+
			"ERROR: Rust FFI library not found\n"+
			"═══════════════════════════════════════════════════════════════\n"+
			"\n"+
			"The Zerobus Go SDK requires a one-time build step:\n"+
			"\n"+
			"  go generate github.com/databricks/zerobus-sdk/go\n"+
			"\n"+
			"Or if you're developing locally:\n"+
			"\n"+
			"  go generate\n"+
			"\n"+
			"Prerequisites:\n"+
			"  - Rust 1.70+ (https://rustup.rs)\n"+
			"  - Optional: cargo-zigbuild for cross-compilation\n"+
			"\n"+
			"This builds the Rust FFI library (takes 2-5 minutes first time).\n"+
			"After that, regular 'go build' will work normally.\n"+
			"\n"+
			"═══════════════════════════════════════════════════════════════\n\n")
	}
}

// ensureRustLibrary checks if the Rust library exists and builds it if needed
func ensureRustLibrary() error {
	// Get the directory of this source file
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return fmt.Errorf("failed to determine source directory")
	}
	sdkDir := filepath.Dir(filename)
	libPath := filepath.Join(sdkDir, "lib", fmt.Sprintf("%s_%s", runtime.GOOS, runtime.GOARCH), "libzerobus_ffi.a")

	// Check if library already exists
	if _, err := os.Stat(libPath); err == nil {
		// Library exists, check if it needs rebuilding
		if !needsRebuild(sdkDir, libPath) {
			return nil
		}
	}

	fmt.Println("Building Rust FFI library (first time or after update)...")
	fmt.Println("This may take 2-5 minutes...")

	return buildRustLibrary(sdkDir)
}

// needsRebuild checks if any Rust source file is newer than the library
func needsRebuild(sdkDir, libPath string) bool {
	libInfo, err := os.Stat(libPath)
	if err != nil {
		return true
	}

	ffiDir := filepath.Join(sdkDir, "..", "rust", "ffi", "src")
	needsRebuild := false

	filepath.Walk(ffiDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return nil
		}
		if filepath.Ext(path) == ".rs" && info.ModTime().After(libInfo.ModTime()) {
			needsRebuild = true
			return filepath.SkipAll
		}
		return nil
	})

	return needsRebuild
}

// buildRustLibrary builds the Rust FFI library
func buildRustLibrary(sdkDir string) error {
	ffiDir := filepath.Join(sdkDir, "..", "rust", "ffi")

	// Check if Rust is installed
	if _, err := exec.LookPath("cargo"); err != nil {
		return fmt.Errorf("cargo not found. Install Rust from https://rustup.rs")
	}

	// Determine build command based on platform
	var cmd *exec.Cmd
	if runtime.GOOS == "windows" {
		// On Windows, build for GNU target to be compatible with MinGW
		fmt.Println("Building for Windows GNU target (MinGW compatible)...")
		cmd = exec.Command("cargo", "build", "--release", "--target", "x86_64-pc-windows-gnu")
	} else if _, err := exec.LookPath("cargo-zigbuild"); err == nil {
		fmt.Println("Using cargo-zigbuild for optimized cross-compilation...")
		cmd = exec.Command("cargo", "zigbuild", "--release")
	} else {
		fmt.Println("Using cargo (install cargo-zigbuild for better cross-compilation)...")
		cmd = exec.Command("cargo", "build", "--release")
	}

	cmd.Dir = ffiDir
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cargo build failed: %w", err)
	}

	// Copy library to SDK directory (handle multiple possible locations)
	dstDir := filepath.Join(sdkDir, "lib", fmt.Sprintf("%s_%s", runtime.GOOS, runtime.GOARCH))
	if err := os.MkdirAll(dstDir, 0755); err != nil {
		return fmt.Errorf("failed to create library directory: %w", err)
	}
	dstLib := filepath.Join(dstDir, "libzerobus_ffi.a")

	// rust/ffi is part of the rust/ workspace, so Cargo outputs to rust/target/
	workspaceTargetDir := filepath.Join(filepath.Dir(ffiDir), "target")
	possiblePaths := []string{
		filepath.Join(workspaceTargetDir, "release", "libzerobus_ffi.a"),
		filepath.Join(workspaceTargetDir, "x86_64-pc-windows-gnu", "release", "libzerobus_ffi.a"),
		filepath.Join(workspaceTargetDir, "release", "zerobus_ffi.lib"),
	}

	var data []byte
	var err error
	var srcLib string

	for _, path := range possiblePaths {
		data, err = os.ReadFile(path)
		if err == nil {
			srcLib = path
			break
		}
	}

	if err != nil {
		return fmt.Errorf("failed to read built library (tried multiple locations): %w", err)
	}

	fmt.Printf("Using library: %s\n", srcLib)

	if err := os.WriteFile(dstLib, data, 0644); err != nil {
		return fmt.Errorf("failed to copy library: %w", err)
	}

	fmt.Println("✓ Rust FFI library built successfully")
	return nil
}
