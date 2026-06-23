#include "zerobus/sdk.hpp"

#include <gtest/gtest.h>

#include <string>

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
