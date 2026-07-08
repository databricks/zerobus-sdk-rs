#ifndef ZEROBUS_ACK_CALLBACK_HPP
#define ZEROBUS_ACK_CALLBACK_HPP

#include <cstdint>
#include <string>

namespace zerobus {

/// Receives asynchronous acknowledgment and error notifications for a stream.
///
/// Register an implementation via `StreamOptions::ack_callback` to be told when
/// records are durably acknowledged by the server, or when they fail, without
/// blocking in `wait_for_offset()` / `flush()`. This is the recommended way to
/// track durability on a continuous, high-throughput stream: keep ingesting and
/// let the callback report progress out of band.
///
/// # Delivery semantics (matching the C FFI / Rust core)
///
/// - `on_ack` is invoked once per record, in offset order. Acks are monotonic:
///   an ack for offset `N` implies every offset `<= N` is acknowledged.
/// - `on_error` is invoked for each record left unacknowledged when the stream
///   fails terminally. The same failure may also surface from an `ingest_*`,
///   `flush()`, or `close()` call. The message is the core error text as-is; it
///   carries no retryability classification.
///
/// # Thread safety and performance
///
/// Callbacks run serialized on a dedicated background task inside the core, so
/// `on_ack` / `on_error` are never invoked concurrently with each other. They
/// do, however, run on a different thread than the one driving the stream, so
/// an implementation must synchronize access to any state it shares with that
/// thread. Keep the callbacks lightweight (e.g. logging, metrics, handing work
/// to a queue); heavy work done inline can build up a callback backlog and
/// stall delivery.
///
/// # Lifetime
///
/// The callback must outlive the `Stream`, which holds a `shared_ptr` to it.
/// The core may invoke callbacks up to (and during) `close()`, but never after
/// `close()` returns.
///
/// # Exceptions
///
/// Do not let exceptions escape `on_ack` / `on_error`: they cannot cross the C
/// FFI boundary. The SDK contains any that do at the boundary (they are caught
/// and dropped) rather than let them unwind into the Rust core, but relying on
/// that is a bug — handle errors inside the callback.
class AckCallback {
 public:
  virtual ~AckCallback() = default;

  /// Called when the record at @p offset has been durably acknowledged.
  virtual void on_ack(std::int64_t offset) = 0;

  /// Called when the record at @p offset failed terminally.
  ///
  /// @param offset The logical offset of the failed record.
  /// @param error_message Human-readable error text from the core.
  virtual void on_error(std::int64_t offset,
                        const std::string& error_message) = 0;
};

}  // namespace zerobus

#endif  // ZEROBUS_ACK_CALLBACK_HPP
