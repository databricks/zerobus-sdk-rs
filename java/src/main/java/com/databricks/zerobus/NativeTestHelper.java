package com.databricks.zerobus;

/**
 * Native test helpers for JNI integration testing.
 *
 * <p>Provides native methods that test callbacks and class resolution from Tokio daemon threads.
 *
 * @apiNote This class is not part of the public SDK API. It lives in {@code src/main} because it
 *     must be loadable by the isolated {@link java.net.URLClassLoader} in {@code
 *     ClassLoaderIsolationRunner}, which cannot see classes on the test classpath. The native
 *     symbols are only compiled when the Rust {@code test-helpers} cargo feature is enabled.
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

  /**
   * Exercises {@link HeadersProvider} callbacks on one native blocking thread.
   *
   * @param provider the stateful provider used by the native test
   * @return {@code "OK"} on success, otherwise an error description
   */
  public static native String nativeTestHeadersProviderCallbacks(HeadersProvider provider);

  /**
   * Verifies that timed-out callbacks remain serialized until their blocking JNI call returns.
   *
   * @param provider the stateful provider used by the native test
   * @return {@code "OK"} on success, otherwise an error description
   */
  public static native String nativeTestHeadersProviderSerialization(HeadersProvider provider);
}
