package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ZerobusSdk} construction. Construction only parses the endpoints, so dummy
 * URLs suffice; runs against the staged JNI library in CI and skips on bare local runs.
 */
public class ZerobusSdkTest {

  private static final String SERVER_ENDPOINT =
      "https://1234567890.zerobus.us-west-2.cloud.databricks.com";
  private static final String UNITY_CATALOG_ENDPOINT =
      "https://test-workspace.cloud.databricks.com";

  /**
   * Skips the test unless the native library is loadable (needed to construct {@link ZerobusSdk}).
   */
  private static void assumeNativeLibrary() {
    boolean available;
    try {
      NativeLoader.ensureLoaded();
      available = true;
    } catch (UnsatisfiedLinkError | ExceptionInInitializerError e) {
      available = false;
    }
    assumeTrue(available, "Native library required to construct ZerobusSdk");
  }

  @Test
  void constructsWithoutApplicationName() {
    assumeNativeLibrary();
    try (ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT)) {
      assertNotNull(sdk);
    }
  }

  @Test
  void constructsWithNullApplicationName() {
    assumeNativeLibrary();
    try (ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT, null)) {
      assertNotNull(sdk);
    }
  }

  @Test
  void constructsWithApplicationName() {
    assumeNativeLibrary();
    try (ZerobusSdk sdk = new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT, "my-app/1.0")) {
      assertNotNull(sdk);
    }
  }

  @Test
  void constructsWithSharedConnectionMode() {
    assumeNativeLibrary();
    try (ZerobusSdk sdk =
        new ZerobusSdk(SERVER_ENDPOINT, UNITY_CATALOG_ENDPOINT, "my-app/1.0", false)) {
      assertNotNull(sdk);
    }
  }

  @Test
  void preservesNativeCreationSignatures() throws Exception {
    assertNotNull(
        ZerobusSdk.class.getDeclaredMethod(
            "nativeCreate", String.class, String.class, String.class, boolean.class));
    assertNotNull(
        ZerobusSdk.class.getDeclaredMethod(
            "nativeCreateStream",
            long.class,
            String.class,
            byte[].class,
            String.class,
            String.class,
            Object.class,
            boolean.class));
    assertNotNull(
        ZerobusSdk.class.getDeclaredMethod(
            "nativeCreateArrowStream",
            long.class,
            String.class,
            byte[].class,
            String.class,
            String.class,
            Object.class));
  }
}
