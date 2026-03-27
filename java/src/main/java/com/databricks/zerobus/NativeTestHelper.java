package com.databricks.zerobus;

/**
 * Native test helper for classloader isolation testing.
 *
 * <p>Provides native methods that test class resolution from Tokio daemon threads, used by {@code
 * ClassLoaderIsolationTest} to verify that cached GlobalRefs work across classloader boundaries.
 */
public class NativeTestHelper {
  static {
    NativeLoader.ensureLoaded();
  }

  /**
   * Attempt to find a class by name from a Tokio daemon thread using direct {@code FindClass}.
   *
   * @param className the JNI class name (e.g. "com/databricks/zerobus/NonRetriableException")
   * @return "OK" if found, or an error message
   */
  public static native String nativeTestFindClassFromDaemonThread(String className);

  /**
   * Attempt to find a class by name from a Tokio daemon thread using the cached GlobalRef.
   *
   * @param className the JNI class name (e.g. "com/databricks/zerobus/NonRetriableException")
   * @return "OK" if found in cache, or an error message
   */
  public static native String nativeTestFindClassFromDaemonThreadCached(String className);
}
