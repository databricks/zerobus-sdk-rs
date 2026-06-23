#include "zerobus/error.hpp"

#include <gtest/gtest.h>

#include <string>

using zerobus::ZerobusException;

TEST(ZerobusException, CarriesMessageAndRetryableTrue) {
  ZerobusException e("transient failure", true);
  EXPECT_STREQ(e.what(), "transient failure");
  EXPECT_TRUE(e.is_retryable());
}

TEST(ZerobusException, CarriesRetryableFalse) {
  ZerobusException e("permanent failure", false);
  EXPECT_STREQ(e.what(), "permanent failure");
  EXPECT_FALSE(e.is_retryable());
}

TEST(ZerobusException, IsCatchableAsStdException) {
  try {
    throw ZerobusException("boom", false);
  } catch (const std::exception& e) {
    EXPECT_STREQ(e.what(), "boom");
    return;
  }
  FAIL() << "expected to catch as std::exception";
}
