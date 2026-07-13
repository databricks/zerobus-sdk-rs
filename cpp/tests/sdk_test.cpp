// Unit tests for Sdk and SdkBuilder. Building an Sdk only stores the endpoint
// and defers the channel (the Rust core holds a lazily-connected
// shared_channel), so construction performs no network I/O and is safe as a
// unit test. The create_stream / create_arrow_stream cases here all hit the
// wrapper's argument-validation guards (null headers provider, empty schema),
// which throw in pure C++ before any FFI/connection attempt.
//
// Runtime ingest/ack paths that require a live endpoint are out of scope (they
// need a mock/real server; tracked separately, cf. issue #469).

#include "zerobus/sdk.hpp"

#include <cstdint>
#include <cstdio>
#include <memory>
#include <string>
#include <vector>

#include "zerobus/error.hpp"
#include "zerobus/headers_provider.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

zerobus::Sdk build_local_sdk() {
  return zerobus::Sdk::builder()
      .endpoint("http://localhost:50051")
      .unity_catalog_url("http://localhost:8080")
      .disable_tls()
      .build();
}

}  // namespace

int main() {
  using zerobus::HeadersProvider;
  using zerobus::Sdk;
  using zerobus::TableProperties;
  using zerobus::ZerobusException;

  // Building an SDK against a never-contacted endpoint succeeds (no network).
  {
    try {
      Sdk sdk = build_local_sdk();
      (void)sdk;
    } catch (const ZerobusException& e) {
      fail("building an SDK against a local endpoint threw");
      std::fprintf(stderr, "  what(): %s\n", e.what());
    }
  }

  // Move transfers the handle; destroying both the moved-from and moved-to SDK
  // must be safe (no double-free) — the moved-from handle is nulled.
  {
    try {
      Sdk sdk = build_local_sdk();
      Sdk moved = std::move(sdk);
      (void)moved;
    } catch (const ZerobusException&) {
      fail("move of a built SDK threw");
    }
  }

  // create_stream with a null headers provider is rejected by the wrapper's
  // guard before any FFI call.
  {
    Sdk sdk = build_local_sdk();
    TableProperties table;
    table.table_name = "main.analytics.events";
    std::shared_ptr<HeadersProvider> provider;  // null
    bool threw = false;
    try {
      (void)sdk.create_stream(table, provider);
    } catch (const ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("create_stream did not reject a null headers provider");
    }
  }

  // create_arrow_stream with an empty schema is rejected (an Arrow stream has
  // no JSON fallback, so the schema bytes are required).
  {
    Sdk sdk = build_local_sdk();
    std::vector<std::uint8_t> empty_schema;
    bool threw = false;
    try {
      (void)sdk.create_arrow_stream("main.analytics.events", empty_schema,
                                    "client-id", "client-secret");
    } catch (const ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("create_arrow_stream did not reject an empty schema");
    }
  }

  // create_arrow_stream with a null headers provider is rejected before the FFI
  // call, even when the schema is non-empty.
  {
    Sdk sdk = build_local_sdk();
    std::vector<std::uint8_t> schema = {1};
    std::shared_ptr<HeadersProvider> provider;  // null
    bool threw = false;
    try {
      (void)sdk.create_arrow_stream("main.analytics.events", schema, provider);
    } catch (const ZerobusException&) {
      threw = true;
    }
    if (!threw) {
      fail("create_arrow_stream did not reject a null headers provider");
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("Sdk builds without network and validates create_* arguments\n");
  return 0;
}
