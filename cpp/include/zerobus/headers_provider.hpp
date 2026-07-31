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
/// Lifetime: you pass the provider as a `shared_ptr` and its ownership is
/// handed to the SDK, which keeps it alive until the Rust core is done with it
/// — after any in-flight `get_headers()` call (including one running during
/// connection recovery) has returned. You therefore do not need to keep your
/// own reference alive past `create_stream`, and a slow `get_headers()` racing
/// stream teardown can no longer be invoked on a freed provider.
///
/// Because the SDK owns the provider, its **destructor may run on an internal
/// SDK worker thread** (whichever thread drops the last reference), not the
/// thread that called `create_stream` / destroyed the `Stream`. Keep
/// `~YourProvider()` non-blocking and free of thread-affine work, and do not
/// let it throw (it runs across the C FFI boundary, where an escaping exception
/// terminates the process).
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
