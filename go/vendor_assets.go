// This file exists only so `go mod vendor` preserves the prebuilt FFI
// archives in lib/<GOOS>_<GOARCH>/, which are referenced by ffi.go via
// `#cgo LDFLAGS`. cgo path strings are invisible to the vendor tool's
// dependency analysis, but `//go:embed` directives are. The `//go:build
// ignore` tag keeps the archives from being linked into any binary as
// embedded bytes.
//
//go:build ignore

package zerobus

import "embed"

//go:embed lib/*/libzerobus_ffi.a
var _ embed.FS
