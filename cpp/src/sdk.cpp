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

const std::uint8_t* non_empty_ptr(const std::vector<std::uint8_t>& bytes) {
  if (bytes.empty()) {
    return nullptr;
  }
  return bytes.data();
}

}  // namespace

// ---------------------------------------------------------------------------
// SdkBuilder
// ---------------------------------------------------------------------------

SdkBuilder::SdkBuilder() : builder_(zerobus_sdk_builder_new()) {
  // Identify this wrapper in the user-agent by default.
  auto* b = static_cast<CZerobusSdkBuilder*>(builder_);
  std::string id = std::string("zerobus-sdk-cpp/") + ZEROBUS_CPP_VERSION;
  zerobus_sdk_builder_sdk_identifier(b, id.c_str());
}

SdkBuilder::~SdkBuilder() {
  if (builder_ != nullptr) {
    zerobus_sdk_builder_free(static_cast<CZerobusSdkBuilder*>(builder_));
    builder_ = nullptr;
  }
}

SdkBuilder::SdkBuilder(SdkBuilder&& other) noexcept : builder_(other.builder_) {
  other.builder_ = nullptr;
}

SdkBuilder& SdkBuilder::operator=(SdkBuilder&& other) noexcept {
  if (this != &other) {
    if (builder_ != nullptr) {
      zerobus_sdk_builder_free(static_cast<CZerobusSdkBuilder*>(builder_));
    }
    builder_ = other.builder_;
    other.builder_ = nullptr;
  }
  return *this;
}

SdkBuilder& SdkBuilder::endpoint(const std::string& value) {
  zerobus_sdk_builder_endpoint(static_cast<CZerobusSdkBuilder*>(builder_),
                               value.c_str());
  return *this;
}

SdkBuilder& SdkBuilder::unity_catalog_url(const std::string& value) {
  zerobus_sdk_builder_unity_catalog_url(
      static_cast<CZerobusSdkBuilder*>(builder_), value.c_str());
  return *this;
}

SdkBuilder& SdkBuilder::sdk_identifier(const std::string& value) {
  zerobus_sdk_builder_sdk_identifier(static_cast<CZerobusSdkBuilder*>(builder_),
                                     value.c_str());
  return *this;
}

SdkBuilder& SdkBuilder::application_name(const std::string& value) {
  zerobus_sdk_builder_application_name(
      static_cast<CZerobusSdkBuilder*>(builder_), value.c_str());
  return *this;
}

SdkBuilder& SdkBuilder::disable_tls() {
  zerobus_sdk_builder_disable_tls(static_cast<CZerobusSdkBuilder*>(builder_));
  return *this;
}

Sdk SdkBuilder::build() {
  detail::ResultGuard guard;
  // zerobus_sdk_builder_build consumes (frees) the builder on both paths.
  CZerobusSdk* sdk = zerobus_sdk_builder_build(
      static_cast<CZerobusSdkBuilder*>(builder_), guard.ptr());
  builder_ = nullptr;
  if (sdk == nullptr) {
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

Sdk Sdk::create(const std::string& endpoint,
                const std::string& unity_catalog_url) {
  detail::ResultGuard guard;
  CZerobusSdk* sdk =
      zerobus_sdk_new(endpoint.c_str(), unity_catalog_url.c_str(), guard.ptr());
  if (sdk == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create Zerobus SDK", false);
  }
  return Sdk(sdk);
}

Sdk::~Sdk() {
  if (handle_ != nullptr) {
    zerobus_sdk_free(handle_);
    handle_ = nullptr;
  }
}

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

Stream Sdk::create_stream(const TableProperties& table,
                          const std::string& client_id,
                          const std::string& client_secret,
                          const StreamOptions& options) {
  detail::ResultGuard guard;
  CStreamConfigurationOptions copts = detail::to_c(options);
  CZerobusStream* stream = zerobus_sdk_create_stream(
      handle_, table.table_name.c_str(), descriptor_ptr(table.descriptor_proto),
      table.descriptor_proto.size(), client_id.c_str(), client_secret.c_str(),
      &copts, guard.ptr());
  if (stream == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create stream", false);
  }
  return Stream(stream, nullptr);
}

Stream Sdk::create_stream(const TableProperties& table,
                          std::shared_ptr<HeadersProvider> headers_provider,
                          const StreamOptions& options) {
  if (headers_provider == nullptr) {
    throw ZerobusException("headers_provider must not be null", false);
  }
  detail::ResultGuard guard;
  CStreamConfigurationOptions copts = detail::to_c(options);
  // The trampoline receives the raw provider pointer; the Stream keeps the
  // shared_ptr alive for as long as the stream exists.
  CZerobusStream* stream = zerobus_sdk_create_stream_with_headers_provider(
      handle_, table.table_name.c_str(), descriptor_ptr(table.descriptor_proto),
      table.descriptor_proto.size(), detail::zerobus_cpp_headers_trampoline,
      headers_provider.get(), &copts, guard.ptr());
  if (stream == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create stream", false);
  }
  return Stream(stream, std::move(headers_provider));
}

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
  const std::uint8_t* schema_ptr = non_empty_ptr(schema_ipc_bytes);
  CArrowStream* stream = zerobus_sdk_create_arrow_stream(
      handle_, table_name.c_str(), schema_ptr,
      schema_ipc_bytes.size(), client_id.c_str(), client_secret.c_str(), &copts,
      guard.ptr());
  if (stream == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create Arrow stream", false);
  }
  return ArrowStream(stream, nullptr);
}

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
  const std::uint8_t* schema_ptr = non_empty_ptr(schema_ipc_bytes);
  CArrowStream* stream = zerobus_sdk_create_arrow_stream_with_headers_provider(
      handle_, table_name.c_str(), schema_ptr,
      schema_ipc_bytes.size(), detail::zerobus_cpp_headers_trampoline,
      headers_provider.get(), &copts, guard.ptr());
  if (stream == nullptr) {
    guard.throw_if_error();
    throw ZerobusException("failed to create Arrow stream", false);
  }
  return ArrowStream(stream, std::move(headers_provider));
}

}  // namespace zerobus
