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
/// Lifetime: the provider must outlive the `Stream`, which holds a `shared_ptr`
/// to it. This is necessary but not always sufficient: a `get_headers()` call
/// still running when `close()` times out (~1s) can be invoked on a freed
/// provider. Keep `get_headers()` well under that budget, or keep the provider
/// alive past the `Stream`.
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
