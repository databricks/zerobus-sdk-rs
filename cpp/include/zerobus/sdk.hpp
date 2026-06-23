#ifndef ZEROBUS_SDK_HPP
#define ZEROBUS_SDK_HPP

#include <cstdint>
#include <memory>
#include <string>
#include <vector>

#include "zerobus/arrow_stream.hpp"
#include "zerobus/config.hpp"
#include "zerobus/headers_provider.hpp"
#include "zerobus/stream.hpp"

namespace zerobus {

struct CZerobusSdk;  // opaque FFI handle (defined in zerobus.h)

/// Identifies the target table and (for proto streams) its schema.
struct TableProperties {
  /// Fully-qualified table name: `catalog.schema.table`.
  std::string table_name;
  /// Serialized protobuf `DescriptorProto`. Leave empty for a JSON stream.
  std::vector<std::uint8_t> descriptor_proto;
};

class Sdk;

/// Fluent builder for `Sdk`. Obtain one from `Sdk::builder()`.
///
/// At minimum, set `endpoint()`. The Unity Catalog URL is required for the
/// OAuth client-credentials path but optional when a custom `HeadersProvider`
/// supplies authentication.
class SdkBuilder {
 public:
  SdkBuilder();
  ~SdkBuilder();

  SdkBuilder(SdkBuilder&& other) noexcept;
  SdkBuilder& operator=(SdkBuilder&& other) noexcept;
  SdkBuilder(const SdkBuilder&) = delete;
  SdkBuilder& operator=(const SdkBuilder&) = delete;

  /// Zerobus gRPC endpoint URL (required).
  SdkBuilder& endpoint(const std::string& value);
  /// Unity Catalog URL. Optional when using a custom headers provider.
  SdkBuilder& unity_catalog_url(const std::string& value);
  /// Override the SDK prefix of the `user-agent` header. Defaults to
  /// `zerobus-sdk-cpp/<version>`.
  SdkBuilder& sdk_identifier(const std::string& value);
  /// Append an application identifier to the `user-agent` header.
  SdkBuilder& application_name(const std::string& value);
  /// Select a plaintext (non-TLS) gRPC channel. TLS is on by default.
  SdkBuilder& disable_tls();

  /// Consume the builder and construct the `Sdk`. Throws `ZerobusException` on
  /// failure. The builder must not be used afterwards.
  Sdk build();

 private:
  void* builder_;  // CZerobusSdkBuilder* (opaque)
};

/// Entry point to the Zerobus SDK: an authenticated connection factory that
/// creates ingestion streams.
///
/// Move-only; the destructor frees the underlying FFI resources. A single `Sdk`
/// may create many streams.
class Sdk {
 public:
  /// Start building an `Sdk`. Preferred over `create()`.
  static SdkBuilder builder();

  /// Convenience constructor mirroring the legacy `zerobus_sdk_new` path: TLS
  /// on, default user agent. Prefer `builder()` for full control.
  static Sdk create(const std::string& endpoint,
                    const std::string& unity_catalog_url);

  ~Sdk();

  Sdk(Sdk&& other) noexcept;
  Sdk& operator=(Sdk&& other) noexcept;
  Sdk(const Sdk&) = delete;
  Sdk& operator=(const Sdk&) = delete;

  /// Create a proto/JSON stream authenticated with OAuth client credentials.
  Stream create_stream(const TableProperties& table,
                       const std::string& client_id,
                       const std::string& client_secret,
                       const StreamOptions& options = {});

  /// Create a proto/JSON stream authenticated with a custom headers provider.
  Stream create_stream(const TableProperties& table,
                       std::shared_ptr<HeadersProvider> headers_provider,
                       const StreamOptions& options = {});

  /// Create an Arrow Flight stream (Beta) authenticated with OAuth client
  /// credentials. `schema_ipc_bytes` is an Arrow IPC stream encoding only the
  /// schema (an empty IPC stream with just the schema message).
  ArrowStream create_arrow_stream(
      const std::string& table_name,
      const std::vector<std::uint8_t>& schema_ipc_bytes,
      const std::string& client_id, const std::string& client_secret,
      const ArrowStreamOptions& options = {});

  /// Create an Arrow Flight stream (Beta) authenticated with a custom headers
  /// provider.
  ArrowStream create_arrow_stream(
      const std::string& table_name,
      const std::vector<std::uint8_t>& schema_ipc_bytes,
      std::shared_ptr<HeadersProvider> headers_provider,
      const ArrowStreamOptions& options = {});

 private:
  friend class SdkBuilder;
  explicit Sdk(CZerobusSdk* handle) : handle_(handle) {}

  CZerobusSdk* handle_;
};

}  // namespace zerobus

#endif  // ZEROBUS_SDK_HPP
