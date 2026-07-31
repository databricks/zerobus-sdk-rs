package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class HeadersProviderNativeTest {

  @Test
  void callbacksPreserveRetryabilityAndClearPendingExceptions() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger invalidations = new AtomicInteger();

    HeadersProvider provider =
        new HeadersProvider() {
          @Override
          public Map<String, String> getHeaders() throws Exception {
            switch (calls.getAndIncrement()) {
              case 0:
                throw new BrokenToStringException();
              case 1:
                return Collections.singletonMap("Authorization", "Bearer token");
              case 2:
                throw new NonRetriableException("permanent failure");
              case 3:
                return nonStringMap();
              default:
                return Collections.singletonMap("invalid header", "value");
            }
          }

          @Override
          public void invalidate() {
            invalidations.incrementAndGet();
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestHeadersProviderCallbacks(provider));
    assertEquals(1, invalidations.get());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Map<String, String> nonStringMap() {
    Map map = new HashMap();
    map.put(1, 2);
    return map;
  }

  private static final class BrokenToStringException extends Exception {
    @Override
    public String toString() {
      throw new IllegalStateException("toString failed");
    }
  }
}
