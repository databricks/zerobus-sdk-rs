#include "zerobus/sdk.hpp"

#include <gtest/gtest.h>

#include <string>
#include <vector>

#include "zerobus/error.hpp"
#include "zerobus/version.hpp"

TEST(Version, MatchesHeaderMacro) {
  EXPECT_STREQ(zerobus::version(), ZEROBUS_CPP_VERSION);
  EXPECT_STREQ(zerobus::version(), "0.1.0");
}

// Building an SDK constructs configuration and a (lazily connected) channel; it
// does not perform any network I/O, so this is safe as a unit test.
TEST(SdkBuilder, BuildsWithoutNetwork) {
  zerobus::Sdk sdk = zerobus::Sdk::builder()
                         .endpoint("http://localhost:50051")
                         .unity_catalog_url("http://localhost:8080")
                         .disable_tls()
                         .build();
  SUCCEED();
}

TEST(SdkBuilder, MoveTransfersOwnership) {
  zerobus::Sdk sdk = zerobus::Sdk::builder()
                         .endpoint("http://localhost:50051")
                         .disable_tls()
                         .build();
  zerobus::Sdk moved = std::move(sdk);
  // Destruction of both the moved-from and moved-to SDK must be safe (no
  // double-free): the moved-from handle is null.
  SUCCEED();
}

TEST(Sdk, CreateStreamRejectsNullHeadersProvider) {
  zerobus::Sdk sdk = zerobus::Sdk::builder()
                         .endpoint("http://localhost:50051")
                         .disable_tls()
                         .build();
  zerobus::TableProperties table;
  table.table_name = "main.analytics.events";

  std::shared_ptr<zerobus::HeadersProvider> provider;
  EXPECT_THROW((void)sdk.create_stream(table, provider), zerobus::ZerobusException);
}

TEST(Sdk, CreateArrowStreamRejectsEmptySchema) {
  zerobus::Sdk sdk = zerobus::Sdk::builder()
                         .endpoint("http://localhost:50051")
                         .disable_tls()
                         .build();
  std::vector<std::uint8_t> empty_schema;
  EXPECT_THROW((void)sdk.create_arrow_stream("main.analytics.events", empty_schema,
                                             "client-id", "client-secret"),
               zerobus::ZerobusException);
}

TEST(Sdk, CreateArrowStreamRejectsNullHeadersProvider) {
  zerobus::Sdk sdk = zerobus::Sdk::builder()
                         .endpoint("http://localhost:50051")
                         .disable_tls()
                         .build();
  std::vector<std::uint8_t> schema = {1};
  std::shared_ptr<zerobus::HeadersProvider> provider;
  EXPECT_THROW(
      (void)sdk.create_arrow_stream("main.analytics.events", schema, provider),
      zerobus::ZerobusException);
}
