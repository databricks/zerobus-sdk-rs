package com.databricks.zerobus;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.Locale;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for loading the native Zerobus JNI library.
 *
 * <p>This class handles the extraction and loading of platform-specific native libraries. It
 * supports loading from the classpath (for packaged applications) or from a system path (for
 * development or custom deployments).
 *
 * <p>The native library is loaded automatically when this class is first accessed.
 *
 * <p>Supported platforms:
 *
 * <ul>
 *   <li>Linux glibc x86_64 (linux-x86_64)
 *   <li>Linux glibc aarch64 (linux-aarch64)
 *   <li>Linux musl x86_64 / Alpine (linux-musl-x86_64)
 *   <li>Linux musl aarch64 / Alpine (linux-musl-aarch64)
 *   <li>macOS x86_64 (osx-x86_64)
 *   <li>macOS aarch64 / Apple Silicon (osx-aarch64)
 *   <li>Windows x86_64 (windows-x86_64)
 * </ul>
 *
 * <p>On Linux, the libc flavor (glibc vs musl) is detected automatically. To override detection,
 * set the system property {@code -Dzerobus.libc=musl} or {@code -Dzerobus.libc=glibc}.
 */
public final class NativeLoader {
  private static final Logger logger = LoggerFactory.getLogger(NativeLoader.class);

  private static final String LIBRARY_NAME = "zerobus_jni";
  private static final String NATIVE_RESOURCE_PATH = "/native";

  private static volatile boolean loaded = false;
  private static volatile Throwable loadError = null;

  static {
    try {
      loadNativeLibrary();
      loaded = true;
    } catch (Throwable t) {
      loadError = t;
      logger.error("Failed to load native library", t);
    }
  }

  private NativeLoader() {
    // Utility class.
  }

  /**
   * Ensures the native library is loaded.
   *
   * @throws UnsatisfiedLinkError if the native library could not be loaded
   */
  public static void ensureLoaded() {
    if (!loaded) {
      if (loadError != null) {
        throw new UnsatisfiedLinkError("Native library failed to load: " + loadError.getMessage());
      }
      throw new UnsatisfiedLinkError("Native library not loaded");
    }
  }

  /**
   * Returns whether the native library has been successfully loaded.
   *
   * @return true if loaded, false otherwise
   */
  public static boolean isLoaded() {
    return loaded;
  }

  /**
   * Returns the error that occurred during loading, if any.
   *
   * @return the error, or null if no error occurred
   */
  public static Throwable getLoadError() {
    return loadError;
  }

  private static void loadNativeLibrary() {
    // First, try to load from java.library.path (system library)
    try {
      System.loadLibrary(LIBRARY_NAME);
      logger.info("Loaded native library from system path");
      return;
    } catch (UnsatisfiedLinkError e) {
      logger.debug("Native library not found in system path, trying classpath");
    }

    // Try to load from classpath (packaged in JAR)
    String platform = getPlatformIdentifier();
    String libraryFileName = getLibraryFileName();
    String resourcePath = NATIVE_RESOURCE_PATH + "/" + platform + "/" + libraryFileName;

    try (InputStream in = NativeLoader.class.getResourceAsStream(resourcePath)) {
      if (in == null) {
        throw new UnsatisfiedLinkError(
            "Native library not found in classpath: "
                + resourcePath
                + ". Platform: "
                + platform
                + ". Make sure the correct native JAR is on the classpath.");
      }

      // Extract to a temporary file
      File tempFile = extractToTempFile(in, libraryFileName);
      System.load(tempFile.getAbsolutePath());
      logger.info("Loaded native library from classpath: {}", resourcePath);

    } catch (IOException e) {
      throw new UnsatisfiedLinkError("Failed to extract native library: " + e.getMessage());
    }
  }

  private static File extractToTempFile(InputStream in, String fileName) throws IOException {
    // Create a unique temp directory for this process
    File tempDir = Files.createTempDirectory("zerobus-native-").toFile();
    tempDir.deleteOnExit();

    File tempFile = new File(tempDir, fileName);
    tempFile.deleteOnExit();

    try (OutputStream out = new FileOutputStream(tempFile)) {
      byte[] buffer = new byte[8192];
      int bytesRead;
      while ((bytesRead = in.read(buffer)) != -1) {
        out.write(buffer, 0, bytesRead);
      }
    }

    // Make the library executable on Unix systems
    if (!tempFile.setExecutable(true)) {
      logger.debug("Could not set executable permission on native library");
    }

    return tempFile;
  }

  // Package-private for unit testing.
  static String getPlatformIdentifier() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
    String arch = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

    String osName;
    if (os.contains("linux")) {
      osName = isMuslLinux() ? "linux-musl" : "linux";
    } else if (os.contains("mac") || os.contains("darwin")) {
      osName = "osx";
    } else if (os.contains("windows")) {
      osName = "windows";
    } else {
      throw new UnsatisfiedLinkError("Unsupported operating system: " + os);
    }

    String archName;
    if (arch.equals("amd64") || arch.equals("x86_64")) {
      archName = "x86_64";
    } else if (arch.equals("aarch64") || arch.equals("arm64")) {
      archName = "aarch64";
    } else {
      throw new UnsatisfiedLinkError("Unsupported architecture: " + arch);
    }

    return osName + "-" + archName;
  }

  private static boolean isMuslLinux() {
    String override = System.getProperty("zerobus.libc");
    if (override != null && !override.isEmpty()) {
      if ("musl".equalsIgnoreCase(override)) {
        return true;
      }
      if ("glibc".equalsIgnoreCase(override)) {
        return false;
      }
      throw new UnsatisfiedLinkError(
          "Invalid value for system property zerobus.libc: '"
              + override
              + "'. Expected 'musl' or 'glibc'.");
    }
    // Prefer /proc/self/maps — it reflects the libc actually loaded into THIS process,
    // which is authoritative on hosts that have both glibc and musl tooling installed.
    try (BufferedReader reader = new BufferedReader(new FileReader("/proc/self/maps"))) {
      String line;
      while ((line = reader.readLine()) != null) {
        if (line.contains("ld-musl-") || line.contains("libc.musl-")) {
          return true;
        }
        if (line.contains("ld-linux-") || line.contains("/libc.so.6")) {
          return false;
        }
      }
    } catch (IOException | SecurityException ignored) {
      // /proc/self/maps unavailable (sandboxed JVM / restrictive SecurityManager);
      // fall through to loader-file existence check below.
    }
    try {
      if (new File("/lib/ld-musl-x86_64.so.1").exists()
          || new File("/lib/ld-musl-aarch64.so.1").exists()) {
        return true;
      }
    } catch (SecurityException ignored) {
      // File.exists() blocked by SecurityManager; assume glibc.
    }
    return false;
  }

  private static String getLibraryFileName() {
    String os = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);

    if (os.contains("windows")) {
      return LIBRARY_NAME + ".dll";
    } else if (os.contains("mac") || os.contains("darwin")) {
      return "lib" + LIBRARY_NAME + ".dylib";
    } else {
      return "lib" + LIBRARY_NAME + ".so";
    }
  }
}
