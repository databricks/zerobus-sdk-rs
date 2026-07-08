#ifndef ZEROBUS_ACK_CALLBACK_HPP
#define ZEROBUS_ACK_CALLBACK_HPP

#include <cstdint>
#include <string>

namespace zerobus {

/// Receives async ack/error notifications, so callers track durability without
/// blocking in `wait_for_offset()` / `flush()`. Register via
/// `StreamOptions::ack_callback`.
///
/// `on_ack` fires once per record in offset order, monotonic (offset `N`
/// implies all `<= N` acked); `on_error` fires per record left unacked on
/// terminal failure. Callbacks run serialized on a background task, on a
/// different thread than the stream: synchronize shared state, keep them light.
/// The callback must outlive the `Stream` (held via `shared_ptr`); it may fire
/// until `close()` returns, never after. Don't throw — exceptions can't cross
/// the C boundary (the SDK drops any that do).
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
