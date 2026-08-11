package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Arrays;
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
                throw new AssertionError("fatal failure");
              case 4:
                return nonStringMap();
              case 5:
                return Collections.singletonMap("invalid header", "value");
              default:
                return Collections.singletonMap(oversizedHeaderName(), "value");
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

  @Test
  void timedOutCallbacksRemainSerialized() {
    assumeTrue(NativeLoader.isLoaded(), "Native library not available");
    AtomicInteger calls = new AtomicInteger();
    AtomicInteger activeCalls = new AtomicInteger();
    AtomicInteger maxActiveCalls = new AtomicInteger();

    HeadersProvider provider =
        () -> {
          int active = activeCalls.incrementAndGet();
          maxActiveCalls.accumulateAndGet(active, Math::max);
          try {
            if (calls.getAndIncrement() == 0) {
              Thread.sleep(500);
            }
            return Collections.singletonMap("authorization", "Bearer token");
          } finally {
            activeCalls.decrementAndGet();
          }
        };

    assertEquals("OK", NativeTestHelper.nativeTestHeadersProviderSerialization(provider));
    assertEquals(2, calls.get());
    assertEquals(1, maxActiveCalls.get());
  }

  @SuppressWarnings({"rawtypes", "unchecked"})
  private static Map<String, String> nonStringMap() {
    Map map = new HashMap();
    map.put(1, 2);
    return map;
  }

  private static String oversizedHeaderName() {
    char[] name = new char[65536];
    Arrays.fill(name, 'a');
    return new String(name);
  }

  private static final class BrokenToStringException extends Exception {
    @Override
    public String toString() {
      throw new IllegalStateException("toString failed");
    }
  }
}
