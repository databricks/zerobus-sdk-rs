package com.databricks.zerobus;

import java.io.File;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;

/**
 * Forked-JVM test runner that simulates Spring Boot's classloader isolation.
 *
 * <p>This program is launched by {@code ClassLoaderIsolationTest} in a child JVM where SDK classes
 * are NOT on the system classpath. Instead, they are loaded through a {@link URLClassLoader} whose
 * parent is the extension/platform classloader (cannot see app classes), simulating Spring Boot's
 * {@code LaunchedURLClassLoader}.
 *
 * <p>Exit codes:
 *
 * <ul>
 *   <li>0 = test passed (direct find_class failed, cached GlobalRef succeeded)
 *   <li>1 = test failed
 *   <li>2 = usage error
 * </ul>
 */
public class ClassLoaderIsolationRunner {

  public static void main(String[] args) throws Exception {
    if (args.length == 0) {
      System.err.println("Usage: ClassLoaderIsolationRunner <classpath-entry> ...");
      System.exit(2);
    }

    // Build URLs for the isolated classloader
    URL[] urls = new URL[args.length];
    for (int i = 0; i < args.length; i++) {
      urls[i] = new File(args[i]).toURI().toURL();
    }

    // Create isolated classloader with extension/platform classloader as parent.
    // This parent cannot see application classes on the system classpath,
    // simulating Spring Boot's LaunchedURLClassLoader.
    ClassLoader parent = ClassLoader.getSystemClassLoader().getParent();
    URLClassLoader isolated = new URLClassLoader(urls, parent);

    // Load NativeLoader through the isolated classloader to trigger JNI_OnLoad
    // with the correct classloader context
    Class<?> nativeLoaderClass = isolated.loadClass("com.databricks.zerobus.NativeLoader");
    nativeLoaderClass.getMethod("ensureLoaded").invoke(null);
    System.out.println("Native library loaded via isolated classloader");

    // Load NativeTestHelper through the isolated classloader
    Class<?> testHelperClass = isolated.loadClass("com.databricks.zerobus.NativeTestHelper");

    // Test 1: Direct find_class from daemon thread (expected to FAIL in isolated classloader)
    Method directMethod =
        testHelperClass.getMethod("nativeTestFindClassFromDaemonThread", String.class);
    String directResult =
        (String) directMethod.invoke(null, "com/databricks/zerobus/NonRetriableException");
    System.out.println("Direct find_class result: " + directResult);

    // Test 2: Cached GlobalRef from daemon thread (expected to SUCCEED)
    Method cachedMethod =
        testHelperClass.getMethod("nativeTestFindClassFromDaemonThreadCached", String.class);
    String cachedResult =
        (String) cachedMethod.invoke(null, "com/databricks/zerobus/NonRetriableException");
    System.out.println("Cached GlobalRef result: " + cachedResult);

    // Verify results
    boolean directFailed = !"OK".equals(directResult);
    boolean cachedSucceeded = "OK".equals(cachedResult);

    if (directFailed && cachedSucceeded) {
      System.out.println(
          "PASSED: Direct find_class failed as expected, cached GlobalRef succeeded");
      System.exit(0);
    } else {
      System.err.println("FAILED:");
      if (!directFailed) {
        System.err.println("  Direct find_class unexpectedly succeeded");
      }
      if (!cachedSucceeded) {
        System.err.println("  Cached GlobalRef failed: " + cachedResult);
      }
      System.exit(1);
    }
  }
}
