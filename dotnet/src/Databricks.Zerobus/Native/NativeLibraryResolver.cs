using System.Reflection;
using System.Runtime.InteropServices;

namespace Databricks.Zerobus.Native;

/// <summary>
/// Resolves the native Zerobus FFI library at runtime for the current platform.
/// Supports NuGet RID-based bundling, local development paths, and system-wide installations.
/// On .NET 8+, uses NativeLibrary.SetDllImportResolver. On netstandard2.0, pre-loads via LoadLibrary/dlopen.
/// </summary>
internal static class NativeLibraryResolver
{
    private static bool _initialized;
    private static readonly object _lock = new();

    /// <summary>
    /// Ensures the native library is loaded and ready. Thread-safe, idempotent.
    /// Call once at SDK initialization time.
    /// </summary>
    public static void EnsureLoaded()
    {
        if (_initialized) return;

        lock (_lock)
        {
            if (_initialized) return;

#if NET8_0_OR_GREATER
            var resolver = new DllImportResolver(ResolveNativeLibrary);
            NativeLibrary.SetDllImportResolver(
                typeof(NativeMethods).Assembly, resolver);
#else
            // For netstandard2.0: pre-load the native library manually
            LoadNativeLibrary();
#endif

            _initialized = true;
        }
    }

#if NET8_0_OR_GREATER
    private static IntPtr ResolveNativeLibrary(string libraryName, Assembly assembly, DllImportSearchPath? searchPath)
    {
        if (libraryName != "zerobus_ffi") return IntPtr.Zero;

        foreach (var candidate in GetCandidatePaths())
        {
            if (NativeLibrary.TryLoad(candidate, out var handle))
            {
                return handle;
            }
        }

        if (NativeLibrary.TryLoad("zerobus_ffi", assembly, searchPath, out var defaultHandle))
        {
            return defaultHandle;
        }

        return IntPtr.Zero;
    }
#else
    private static void LoadNativeLibrary()
    {
        foreach (var candidate in GetCandidatePaths())
        {
            if (TryLoadLibrary(candidate))
            {
                return;
            }
        }

        // Try default search paths
        TryLoadLibrary(GetLibraryFileName());
    }

    [DllImport("kernel32", SetLastError = true)]
    private static extern IntPtr LoadLibrary(string lpFileName);

    [DllImport("libdl", SetLastError = true)]
    private static extern IntPtr dlopen(string filename, int flags);

    private static bool TryLoadLibrary(string path)
    {
        try
        {
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                return LoadLibrary(path) != IntPtr.Zero;
            }
            else
            {
                return dlopen(path, 2 /* RTLD_NOW */) != IntPtr.Zero;
            }
        }
        catch
        {
            return false;
        }
    }
#endif

    private static IEnumerable<string> GetCandidatePaths()
    {
        string rid = GetRuntimeIdentifier();
        string libName = GetLibraryFileName();

        string baseDir = AppContext.BaseDirectory;
        yield return Path.Combine(baseDir, "runtimes", rid, "native", libName);

        string? devDir = FindDevRuntimesDir();
        if (devDir != null)
        {
            yield return Path.Combine(devDir, rid, "native", libName);
        }

        string? envPath = Environment.GetEnvironmentVariable("ZEROBUS_NATIVE_LIB_PATH");
        if (!string.IsNullOrEmpty(envPath))
        {
            yield return Path.Combine(envPath, libName);
            yield return envPath;
        }

        yield return Path.Combine(baseDir, libName);
        yield return Path.Combine(baseDir, "../../../../runtimes", rid, "native", libName);
    }

    private static string? FindDevRuntimesDir()
    {
        try
        {
            var asmDir = AppContext.BaseDirectory;
            var dir = new DirectoryInfo(asmDir);
            while (dir != null && dir.Parent != null)
            {
                var candidate = Path.Combine(dir.FullName, "runtimes");
                if (Directory.Exists(candidate))
                {
                    return candidate;
                }

                var srcCandidate = Path.Combine(dir.Parent.FullName, "src", "Databricks.Zerobus", "runtimes");
                if (Directory.Exists(srcCandidate))
                {
                    return srcCandidate;
                }

                dir = dir.Parent;
            }
        }
        catch
        {
            // Ignore failures walking directories
        }
        return null;
    }

    internal static string GetRuntimeIdentifier()
    {
        string os = GetOs();
        string arch = RuntimeInformation.ProcessArchitecture switch
        {
            Architecture.X64 => "x64",
            Architecture.Arm64 => "arm64",
            _ => "x64"
        };
        return $"{os}-{arch}";
    }

    internal static string GetOs()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return "win";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return "linux";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) return "osx";
        return "linux";
    }

    internal static string GetLibraryFileName()
    {
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows)) return "zerobus_ffi.dll";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.Linux)) return "libzerobus_ffi.so";
        if (RuntimeInformation.IsOSPlatform(OSPlatform.OSX)) return "libzerobus_ffi.dylib";
        return "libzerobus_ffi.so";
    }
}
