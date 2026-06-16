# Zerobus C FFI

C Foreign Function Interface bindings for the Zerobus Rust SDK.

## Building

```bash
# Build both static and dynamic libraries
cargo build -p zerobus-ffi --release

# Output:
# target/release/libzerobus_ffi.a    (static library)
# target/release/libzerobus_ffi.so   (Linux dynamic library)
# target/release/libzerobus_ffi.dylib (macOS dynamic library)
# target/release/zerobus_ffi.dll     (Windows dynamic library)
```

## Cross-compilation

```bash
# Linux ARM64
rustup target add aarch64-unknown-linux-gnu
cargo build -p zerobus-ffi --release --target aarch64-unknown-linux-gnu

# macOS ARM64 (Apple Silicon)
rustup target add aarch64-apple-darwin
cargo build -p zerobus-ffi --release --target aarch64-apple-darwin

# Windows
rustup target add x86_64-pc-windows-gnu
cargo build -p zerobus-ffi --release --target x86_64-pc-windows-gnu
```

## Usage

### Go (CGO with static library)

```go
/*
#cgo LDFLAGS: -L${SRCDIR}/lib -lzerobus_ffi -ldl -lpthread -lm
#include "zerobus.h"
*/
import "C"
```

### C# (P/Invoke with dynamic library)

```csharp
[DllImport("zerobus_ffi", CallingConvention = CallingConvention.Cdecl)]
private static extern IntPtr zerobus_sdk_new(string endpoint, string ucUrl, ref CResult result);
```

### C++

```cpp
#include "zerobus.h"

// Link with -lzerobus_ffi
```

### Dynamic protobuf from a Unity Catalog schema (pure C)

Build a protobuf descriptor and encode records straight from Unity Catalog
table metadata — no pre-generated `.proto` file and no second Rust crate:

```c
CResult r = {0};

/* init: fetch GET /api/2.1/unity-catalog/tables/{name} and pass its JSON body */
CZerobusProtoSchema *schema = zerobus_proto_schema_from_uc_json(uc_table_json, &r);
/* on error schema == NULL; read r.error_message then zerobus_free_error_message(r.error_message) */

uintptr_t dlen;
const uint8_t *desc = zerobus_proto_schema_descriptor_bytes(schema, &dlen);
CZerobusStream *stream = zerobus_sdk_create_stream(sdk, table_name, desc, dlen,
                                                   client_id, client_secret, &opts, &r);

/* per record, at flush time */
uint8_t *buf; uintptr_t len;
if (zerobus_proto_schema_encode_json(schema, record_json, &buf, &len, &r)) {
    /* collect buf/len into a batch, ingest via zerobus_stream_ingest_proto_records(...) */
    zerobus_free_proto_bytes(buf, len);
}

/* shutdown */
zerobus_proto_schema_free(schema);
```

Encoding contract: record object keys are matched to column names; unknown keys
are ignored (upstream records often carry extra non-column metadata). `DATE`,
`TIMESTAMP`, and `TIMESTAMP_NTZ` columns are encoded as integers — supply days
(for `DATE`) or microseconds since the Unix epoch (for `TIMESTAMP*`), not
ISO-8601 strings.

## API Reference

See `zerobus.h` for the complete C API documentation.
