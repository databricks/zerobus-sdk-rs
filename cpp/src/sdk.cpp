// Implementation of Sdk and SdkBuilder (declared in zerobus/sdk.hpp).
//
// Each method forwards to the C FFI (zerobus.h): the builder accumulates
// configuration on an opaque CZerobusSdkBuilder and build() consumes it into a
// CZerobusSdk, while create_stream / create_arrow_stream hand the configured
// options across the boundary. Every fallible call routes its CResult through
// detail::ResultGuard, which converts failure into a ZerobusException and frees
// the C error string. Public API documentation lives on the declarations in the
// header; comments here cover only implementation details.
#include "zerobus/sdk.hpp"

#include <utility>

#include "detail/config_convert.hpp"
#include "detail/ffi_util.hpp"
#include "detail/headers_callback.hpp"
#include "zerobus/version.hpp"

namespace zerobus {

namespace {

// Pointer to the descriptor bytes, or null for a JSON stream.
const std::uint8_t* descriptor_ptr(const std::vector<std::uint8_t>& d) {
  return d.empty() ? nullptr : d.data();
}

}  // namespace

// ---------------------------------------------------------------------------
// SdkBuilder
// ---------------------------------------------------------------------------

// Allocate a fresh FFI builder and stamp the default user-agent prefix.
SdkBuilder::SdkBuilder() : builder_(zerobus_sdk_builder_new()) {
  std::string id = std::string("zerobus-sdk-cpp/") + ZEROBUS_CPP_VERSION;
  zerobus_sdk_builder_sdk_identifier(builder_, id.c_str());
}

// Free the builder unless it was already consumed by build() (which nulls it).
SdkBuilder::~SdkBuilder() {
  if (builder_ != nullptr) {
    zerobus_sdk_builder_free(builder_);
    builder_ = nullptr;
  }
}

// Move steals the opaque pointer and nulls the source, so the FFI handle is
// owned — and ultimately freed — by exactly one SdkBuilder.
SdkBuilder::SdkBuilder(SdkBuilder&& other) noexcept : builder_(other.builder_) {
  other.builder_ = nullptr;
}

SdkBuilder& SdkBuilder::operator=(SdkBuilder&& other) noexcept {
  if (this != &other) {
    // Free any builder we already hold before taking over other's.
    if (builder_ != nullptr) {
      zerobus_sdk_builder_free(builder_);
    }
    builder_ = other.builder_;
    other.builder_ = nullptr;
  }
  return *this;
}

// The configuration setters below each forward their value to the matching
// zerobus_sdk_builder_* FFI call and return *this so calls can be chained. They
// are infallible at this layer; an invalid value (e.g. a malformed endpoint)
// surfaces later, when build() is called.
SdkBuilder& SdkBuilder::endpoint(const std::string& value) {
  zerobus_sdk_builder_endpoint(builder_,
                               detail::checked_c_str(value, "endpoint"));
  return *this;
}

SdkBuilder& SdkBuilder::unity_catalog_url(const std::string& value) {
  zerobus_sdk_builder_unity_catalog_url(
      builder_, detail::checked_c_str(value, "unity_catalog_url"));
  return *this;
}

SdkBuilder& SdkBuilder::sdk_identifier(const std::string& value) {
  zerobus_sdk_builder_sdk_identifier(
      builder_, detail::checked_c_str(value, "sdk_identifier"));
  return *this;
}

SdkBuilder& SdkBuilder::application_name(const std::string& value) {
  zerobus_sdk_builder_application_name(
      builder_, detail::checked_c_str(value, "application_name"));
  return *this;
}

SdkBuilder& SdkBuilder::disable_tls() {
  zerobus_sdk_builder_disable_tls(builder_);
  return *this;
}

// Consume the builder into an Sdk. The FFI takes ownership of the builder and
// frees it on both the success and failure paths, so we null builder_ up front
// to keep the destructor from double-freeing it.
Sdk SdkBuilder::build() {
  detail::ResultGuard guard;
  CZerobusSdk* sdk = zerobus_sdk_builder_build(builder_, guard.ptr());
  builder_ = nullptr;
  if (sdk == nullptr) {
    // A null handle means failure: throw the FFI's error if it set one.
    guard.throw_if_error();
    // Fallback if the FFI returned null without an error message.
    throw ZerobusException("failed to build Zerobus SDK", false);
  }
  return Sdk(sdk);
}

// ---------------------------------------------------------------------------
// Sdk
// ---------------------------------------------------------------------------

SdkBuilder Sdk::builder() { return SdkBuilder(); }

// Convenience path equivalent to building with only endpoint + UC URL set.
// Routes through the builder (rather than zerobus_sdk_new) so the user-agent
// carries the C++ SDK identifier; zerobus_sdk_new would leave the Rust default
// (zerobus-sdk-rs/<version>), mis-attributing C++ traffic as Rust.
Sdk Sdk::create(const std::string& endpoint,
                const std::string& unity_catalog_url) {
  return builder()
      .endpoint(endpoint)
      .unity_catalog_url(unity_catalog_url)
      .build();
}

// Release the Rust-owned SDK handle. Streams hold their own handles, so an Sdk
// may be destroyed independently of the streams it created.
Sdk::~Sdk() {
  if (handle_ != nullptr) {
    zerobus_sdk_free(handle_);
    handle_ = nullptr;
  }
}

// Same handle-stealing move contract as SdkBuilder: the source is nulled so the
// handle is freed exactly once.
Sdk::Sdk(Sdk&& other) noexcept : handle_(other.handle_) {
  other.handle_ = nullptr;
}

Sdk& Sdk::operator=(Sdk&& other) noexcept {
  if (this != &other) {
    if (handle_ != nullptr) {
      zerobus_sdk_free(handle_);
    }
    handle_ = other.handle_;
    other.handle_ = nullptr;
  }
  return *this;
}

// Create an OAuth-authenticated proto/JSON stream. The descriptor pointer is
// null for a JSON stream (descriptor_ptr() maps an empty vector to null). No
// headers provider (null second arg); the third arg keeps the ack callback (if
// any) alive for the raw user_data the core holds.
Stream Sdk::create_stream(const TableProperties& table,
                          const std::string& client_id,
                          const std::string& client_secret,
                          const StreamOptions& options) {
  detail::ResultGuard guard;
  CStreamConfigurationOptions copts = detail::to_c(options);
  CZerobusStream* stream = zerobus_sdk_create_stream(
      handle_, detail::checked_c_str(table.table_name, "table_name"),
      descriptor_ptr(table.descriptor_proto), table.descriptor_proto.size(),
      detail::checked_c_str(client_id, "client_id"),
      detail::checked_c_str(client_secret, "client_secret"), &copts,
      guard.ptr());
  if (stream == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create stream", false);
  }
  // No headers provider; keep the ack callback (if any) alive on the Stream.
  return Stream(stream, nullptr, options.ack_callback);
}

// Same as above but authenticated by a custom headers provider. Ownership of
// the provider is handed to the FFI: it is passed as a heap-allocated
// shared_ptr (the trampoline's user_data) which the FFI releases via
// zerobus_cpp_headers_free only after any in-flight get_headers has returned.
// This closes the recovery-vs-teardown use-after-free, so the Stream no longer
// keeps its own provider shared_ptr.
Stream Sdk::create_stream(const TableProperties& table,
                          std::shared_ptr<HeadersProvider> headers_provider,
                          const StreamOptions& options) {
  if (headers_provider == nullptr) {
    throw ZerobusException("headers_provider must not be null", false);
  }
  detail::ResultGuard guard;
  CStreamConfigurationOptions copts = detail::to_c(options);
  // Validate the table name before allocating `owned` so a throw here can't
  // leak it (argument evaluation order vs. the `new` is unspecified).
  const char* c_table = detail::checked_c_str(table.table_name, "table_name");
  // Heap-allocate the owning shared_ptr; the FFI takes ownership and frees it
  // via zerobus_cpp_headers_free on every path (success or failure).
  auto* owned =
      new std::shared_ptr<HeadersProvider>(std::move(headers_provider));
  CZerobusStream* stream = zerobus_sdk_create_stream_with_headers_provider(
      handle_, c_table, descriptor_ptr(table.descriptor_proto),
      table.descriptor_proto.size(), detail::zerobus_cpp_headers_trampoline,
      owned, detail::zerobus_cpp_headers_free, &copts, guard.ptr());
  if (stream == nullptr) {
    // The FFI already freed `owned` via zerobus_cpp_headers_free on the failure
    // path, so we must not delete it here.
    guard.throw_if_error();
    throw ZerobusException("failed to create stream", false);
  }
  // Provider ownership now lives in the FFI; keep only the ack callback alive.
  return Stream(stream, nullptr, options.ack_callback);
}

// Create an OAuth-authenticated Arrow Flight stream (Beta). The schema IPC
// bytes are required (an Arrow stream has no JSON fallback), so reject an empty
// schema before crossing the FFI rather than letting the core fail less
// clearly.
ArrowStream Sdk::create_arrow_stream(
    const std::string& table_name,
    const std::vector<std::uint8_t>& schema_ipc_bytes,
    const std::string& client_id, const std::string& client_secret,
    const ArrowStreamOptions& options) {
  if (schema_ipc_bytes.empty()) {
    throw ZerobusException("schema_ipc_bytes must not be empty", false);
  }
  detail::ResultGuard guard;
  CArrowStreamConfigurationOptions copts = detail::to_c(options);
  // Non-empty checked above, so data() is non-null.
  CArrowStream* stream = zerobus_sdk_create_arrow_stream(
      handle_, detail::checked_c_str(table_name, "table_name"),
      schema_ipc_bytes.data(), schema_ipc_bytes.size(),
      detail::checked_c_str(client_id, "client_id"),
      detail::checked_c_str(client_secret, "client_secret"), &copts,
      guard.ptr());
  if (stream == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create Arrow stream", false);
  }
  return ArrowStream(stream, nullptr);
}

// Arrow Flight stream (Beta) authenticated by a custom headers provider. As
// with the proto path, provider ownership is handed to the FFI (a heap
// shared_ptr freed via zerobus_cpp_headers_free after any in-flight
// get_headers returns), so the ArrowStream no longer keeps its own shared_ptr.
ArrowStream Sdk::create_arrow_stream(
    const std::string& table_name,
    const std::vector<std::uint8_t>& schema_ipc_bytes,
    std::shared_ptr<HeadersProvider> headers_provider,
    const ArrowStreamOptions& options) {
  if (headers_provider == nullptr) {
    throw ZerobusException("headers_provider must not be null", false);
  }
  if (schema_ipc_bytes.empty()) {
    throw ZerobusException("schema_ipc_bytes must not be empty", false);
  }
  detail::ResultGuard guard;
  CArrowStreamConfigurationOptions copts = detail::to_c(options);
  // Validate the table name before allocating `owned` (see the proto path).
  const char* c_table = detail::checked_c_str(table_name, "table_name");
  auto* owned =
      new std::shared_ptr<HeadersProvider>(std::move(headers_provider));
  // Non-empty checked above, so data() is non-null.
  CArrowStream* stream = zerobus_sdk_create_arrow_stream_with_headers_provider(
      handle_, c_table, schema_ipc_bytes.data(), schema_ipc_bytes.size(),
      detail::zerobus_cpp_headers_trampoline, owned,
      detail::zerobus_cpp_headers_free, &copts, guard.ptr());
  if (stream == nullptr) {
    // The FFI already freed `owned` on the failure path; do not delete it here.
    guard.throw_if_error();
    throw ZerobusException("failed to create Arrow stream", false);
  }
  return ArrowStream(stream, nullptr);
}

}  // namespace zerobus
