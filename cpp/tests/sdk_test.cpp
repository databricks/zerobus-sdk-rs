// Sdk / SdkBuilder. build() only stores the endpoint (the channel is lazy), so
// construction does no network I/O; the create_* cases hit the wrapper's
// argument guards (null provider, empty schema) before any FFI call.
//
// Runtime ingest/ack paths need a live endpoint and are out of scope (cf.
// #469).

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

  // Build succeeds with no network.
  {
    try {
      Sdk sdk = build_local_sdk();
      (void)sdk;
    } catch (const ZerobusException& e) {
      fail("building an SDK against a local endpoint threw");
      std::fprintf(stderr, "  what(): %s\n", e.what());
    }
  }

  // Shared-connection mode is an explicit, chainable opt-out.
  {
    try {
      Sdk sdk = Sdk::builder()
                    .endpoint("http://localhost:50051")
                    .unity_catalog_url("http://localhost:8080")
                    .connection_per_stream(false)
                    .disable_tls()
                    .build();
      (void)sdk;
    } catch (const ZerobusException& e) {
      fail("building an SDK in shared-connection mode threw");
      std::fprintf(stderr, "  what(): %s\n", e.what());
    }
  }

  // Move nulls the source handle, so both destruct safely (no double-free).
  {
    try {
      Sdk sdk = build_local_sdk();
      Sdk moved = std::move(sdk);
      (void)moved;
    } catch (const ZerobusException&) {
      fail("move of a built SDK threw");
    }
  }

  // create_stream rejects a null headers provider.
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

  // create_arrow_stream rejects an empty schema (no JSON fallback).
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

  // create_arrow_stream rejects a null headers provider (schema non-empty).
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
