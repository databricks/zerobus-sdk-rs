package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.util.Locale;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link NativeLoader} platform-identifier resolution and the {@code zerobus.libc}
 * override.
 *
 * <p>The libc override paths are Linux-specific; non-Linux hosts skip those tests via {@link
 * org.junit.jupiter.api.Assumptions#assumeTrue}.
 */
public class NativeLoaderTest {

  private static final String OS_NAME = System.getProperty("os.name", "").toLowerCase(Locale.ROOT);
  private static final String OS_ARCH = System.getProperty("os.arch", "").toLowerCase(Locale.ROOT);

  private String savedOverride;

  @BeforeAll
  static void forceStaticInitWithoutOverride() {
    // Trigger NativeLoader's static initializer under the natural environment, so it isn't
    // re-run inside a test method where the override may point at a non-existent .so path.
    String saved = System.getProperty("zerobus.libc");
    System.clearProperty("zerobus.libc");
    try {
      NativeLoader.isLoaded();
    } finally {
      if (saved != null) {
        System.setProperty("zerobus.libc", saved);
      }
    }
  }

  @BeforeEach
  void saveAndClearOverride() {
    savedOverride = System.getProperty("zerobus.libc");
    System.clearProperty("zerobus.libc");
  }

  @AfterEach
  void restoreOverride() {
    if (savedOverride == null) {
      System.clearProperty("zerobus.libc");
    } else {
      System.setProperty("zerobus.libc", savedOverride);
    }
  }

  @Test
  void muslOverridePicksMuslPath() {
    assumeTrue(OS_NAME.contains("linux"), "Linux-only test");
    System.setProperty("zerobus.libc", "musl");
    String id = NativeLoader.getPlatformIdentifier();
    assertEquals("linux-musl-" + expectedArch(), id);
  }

  @Test
  void glibcOverridePicksGlibcPath() {
    assumeTrue(OS_NAME.contains("linux"), "Linux-only test");
    System.setProperty("zerobus.libc", "glibc");
    String id = NativeLoader.getPlatformIdentifier();
    assertEquals("linux-" + expectedArch(), id);
  }

  @Test
  void invalidOverrideThrows() {
    assumeTrue(OS_NAME.contains("linux"), "Linux-only test");
    System.setProperty("zerobus.libc", "msul");
    UnsatisfiedLinkError err =
        assertThrows(UnsatisfiedLinkError.class, NativeLoader::getPlatformIdentifier);
    assertTrue(
        err.getMessage().contains("Invalid value"),
        "Expected clear error message, got: " + err.getMessage());
  }

  @Test
  void overrideIsCaseInsensitive() {
    assumeTrue(OS_NAME.contains("linux"), "Linux-only test");
    System.setProperty("zerobus.libc", "MUSL");
    assertEquals("linux-musl-" + expectedArch(), NativeLoader.getPlatformIdentifier());
    System.setProperty("zerobus.libc", "Glibc");
    assertEquals("linux-" + expectedArch(), NativeLoader.getPlatformIdentifier());
  }

  @Test
  void emptyOverrideFallsThroughToDetection() {
    assumeTrue(OS_NAME.contains("linux"), "Linux-only test");
    System.setProperty("zerobus.libc", "");
    // Should not throw — empty value is ignored, auto-detection runs.
    String id = NativeLoader.getPlatformIdentifier();
    assertTrue(
        id.equals("linux-" + expectedArch()) || id.equals("linux-musl-" + expectedArch()),
        "Unexpected platform id: " + id);
  }

  private static String expectedArch() {
    if (OS_ARCH.equals("amd64") || OS_ARCH.equals("x86_64")) {
      return "x86_64";
    }
    if (OS_ARCH.equals("aarch64") || OS_ARCH.equals("arm64")) {
      return "aarch64";
    }
    throw new IllegalStateException("Unsupported test arch: " + OS_ARCH);
  }
}
