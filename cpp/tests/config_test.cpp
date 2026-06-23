#include <cstdint>

#include "detail/config_convert.hpp"
#include "test_harness.hpp"

using namespace zerobus;

TEST(ConfigConvert, StreamDefaultsMatchDocumented) {
  StreamOptions opts;
  CStreamConfigurationOptions c = detail::to_c(opts);

  EXPECT_EQ(c.max_inflight_requests, static_cast<std::size_t>(1'000'000));
  EXPECT_TRUE(c.recovery);
  EXPECT_EQ(c.recovery_timeout_ms, 15'000u);
  EXPECT_EQ(c.recovery_backoff_ms, 2'000u);
  EXPECT_EQ(c.recovery_retries, 4u);
  EXPECT_EQ(c.server_lack_of_ack_timeout_ms, 60'000u);
  EXPECT_EQ(c.flush_timeout_ms, 300'000u);
  EXPECT_EQ(c.record_type, static_cast<std::int32_t>(RecordType::Proto));
  EXPECT_FALSE(c.has_stream_paused_max_wait_time_ms);
  EXPECT_FALSE(c.has_callback_max_wait_time_ms);
}

TEST(ConfigConvert, StreamRecoveryFalseIsExplicit) {
  // Unlike the Go wrapper, recovery=false is never confused with a zero value.
  StreamOptions opts;
  opts.recovery = false;
  CStreamConfigurationOptions c = detail::to_c(opts);
  EXPECT_FALSE(c.recovery);
}

TEST(ConfigConvert, StreamOverridesAndOptionals) {
  StreamOptions opts;
  opts.max_inflight_requests = 7;
  opts.record_type = RecordType::Json;
  opts.stream_paused_max_wait_time_ms = 42;
  opts.callback_max_wait_time_ms = 9;

  CStreamConfigurationOptions c = detail::to_c(opts);
  EXPECT_EQ(c.max_inflight_requests, static_cast<std::size_t>(7));
  EXPECT_EQ(c.record_type, static_cast<std::int32_t>(RecordType::Json));
  EXPECT_TRUE(c.has_stream_paused_max_wait_time_ms);
  EXPECT_EQ(c.stream_paused_max_wait_time_ms, 42u);
  EXPECT_TRUE(c.has_callback_max_wait_time_ms);
  EXPECT_EQ(c.callback_max_wait_time_ms, 9u);
}

TEST(ConfigConvert, ArrowDefaults) {
  ArrowStreamOptions opts;
  CArrowStreamConfigurationOptions c = detail::to_c(opts);

  EXPECT_EQ(c.max_inflight_batches, static_cast<std::size_t>(1'000));
  EXPECT_TRUE(c.recovery);
  EXPECT_EQ(c.connection_timeout_ms, 30'000u);
  // None compression and "wait full server duration" are both -1 sentinels.
  EXPECT_EQ(c.ipc_compression, -1);
  EXPECT_EQ(c.stream_paused_max_wait_time_ms, -1);
}

TEST(ConfigConvert, ArrowCompressionAndPause) {
  ArrowStreamOptions opts;
  opts.ipc_compression = IpcCompression::Zstd;
  opts.stream_paused_max_wait_time_ms = 100;

  CArrowStreamConfigurationOptions c = detail::to_c(opts);
  EXPECT_EQ(c.ipc_compression, 1);
  EXPECT_EQ(c.stream_paused_max_wait_time_ms, 100);

  opts.ipc_compression = IpcCompression::Lz4Frame;
  EXPECT_EQ(detail::to_c(opts).ipc_compression, 0);
}
