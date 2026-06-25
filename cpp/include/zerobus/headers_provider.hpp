#ifndef ZEROBUS_HEADERS_PROVIDER_HPP
#define ZEROBUS_HEADERS_PROVIDER_HPP

#include <map>
#include <string>

namespace zerobus {

/// Supplies authentication headers for a stream on demand.
///
/// Implement this interface to provide custom authentication (e.g. a rotating
/// bearer token) instead of static OAuth client credentials. The Rust core
/// invokes `get_headers()` whenever it needs fresh headers, possibly from an
/// internal worker thread.
///
/// Thread safety: the core guards against *concurrent* invocations and will
/// surface an error if two calls overlap, but `get_headers()` may still be
/// called from a different thread than the one that created the stream.
/// Implementations should therefore be thread-safe with respect to their own
/// state.
///
/// Lifetime: a provider passed to `Sdk::create_stream` must remain alive for as
/// long as the resulting `Stream`. The `Stream` holds a `shared_ptr` to it, so
/// passing a `shared_ptr` is sufficient.
///
/// WARNING — this lifetime guarantee does not extend to the `Stream`'s
/// fire-and-forget (`_nowait`) ingestion APIs. Those spawn detached background
/// tasks that the core does not join on `close()` or destruction, so a task may
/// invoke `get_headers()` after the `Stream` has dropped its `shared_ptr` and
/// destroyed this provider. Do not combine a `HeadersProvider` with the
/// `_nowait` APIs; use the blocking ingest variants instead.
class HeadersProvider {
 public:
  virtual ~HeadersProvider() = default;

  /// Return the headers to attach to stream requests.
  ///
  /// Throwing from this method propagates the exception message back to the
  /// Rust core as a headers-provider error, which fails the pending operation.
  virtual std::map<std::string, std::string> get_headers() = 0;
};

}  // namespace zerobus

#endif  // ZEROBUS_HEADERS_PROVIDER_HPP
