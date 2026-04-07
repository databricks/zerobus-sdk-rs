package com.databricks.zerobus;

import static org.junit.jupiter.api.Assertions.*;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;

/**
 * Tests that JNI class resolution works correctly when SDK classes are loaded via an isolated
 * classloader (as in Spring Boot's {@code LaunchedURLClassLoader}).
 *
 * <p>This test forks a child JVM where SDK classes are NOT on the system classpath but are loaded
 * through a {@link java.net.URLClassLoader} with the extension/platform classloader as parent. This
 * simulates the Spring Boot classloader hierarchy where {@code FindClass} from daemon threads
 * (which use the system classloader) cannot see SDK classes.
 */
class ClassLoaderIsolationTest {

  @Test
  void classLoaderIsolation() throws Exception {
    Assumptions.assumeTrue(NativeLoader.isLoaded(), "Native library not available, skipping test");

    String classpath = System.getProperty("java.class.path");
    String pathSep = System.getProperty("path.separator");
    String[] entries = classpath.split(Pattern.quote(pathSep));

    // Split classpath:
    // - test-classes → system classpath (so ClassLoaderIsolationRunner is accessible)
    // - everything else → passed as args for URLClassLoader
    List<String> systemCpEntries = new ArrayList<>();
    List<String> isolatedCpEntries = new ArrayList<>();

    for (String entry : entries) {
      if (entry.contains("test-classes")) {
        systemCpEntries.add(entry);
      } else {
        isolatedCpEntries.add(entry);
      }
    }

    Assumptions.assumeTrue(!systemCpEntries.isEmpty(), "Could not find test-classes on classpath");
    Assumptions.assumeTrue(!isolatedCpEntries.isEmpty(), "Could not find SDK classes on classpath");

    // Build the forked JVM command
    String javaHome = System.getProperty("java.home");
    String javaExe = javaHome + File.separator + "bin" + File.separator + "java";

    List<String> command = new ArrayList<>();
    command.add(javaExe);

    // System classpath: only test-classes (ClassLoaderIsolationRunner is here)
    command.add("-cp");
    command.add(String.join(pathSep, systemCpEntries));

    // Forward java.library.path so the native library can be found
    String libraryPath = System.getProperty("java.library.path");
    if (libraryPath != null && !libraryPath.isEmpty()) {
      command.add("-Djava.library.path=" + libraryPath);
    }

    // Main class
    command.add("com.databricks.zerobus.ClassLoaderIsolationRunner");

    // Args: classpath entries for URLClassLoader
    command.addAll(isolatedCpEntries);

    ProcessBuilder pb = new ProcessBuilder(command);
    pb.redirectErrorStream(true);
    Process proc = pb.start();

    // Read output
    BufferedReader reader = new BufferedReader(new InputStreamReader(proc.getInputStream()));
    StringBuilder output = new StringBuilder();
    String line;
    while ((line = reader.readLine()) != null) {
      output.append(line).append(System.lineSeparator());
    }

    boolean finished = proc.waitFor(30, TimeUnit.SECONDS);
    if (!finished) {
      proc.destroyForcibly();
      fail("ClassLoaderIsolationRunner timed out after 30 seconds");
    }
    int exitCode = proc.exitValue();

    System.out.println("=== ClassLoaderIsolationRunner output ===");
    System.out.print(output);
    System.out.println("=== exit code: " + exitCode + " ===");

    assertEquals(0, exitCode, "ClassLoaderIsolationRunner failed:\n" + output);
  }
}
