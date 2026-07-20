using System.Runtime.InteropServices;

namespace Databricks.Zerobus.IntegrationTests;

internal static class NativeLibraryHelper
{
    private static bool? _isAvailable;

    public static bool IsNativeLibraryAvailable()
    {
        if (_isAvailable.HasValue) return _isAvailable.Value;

        try
        {
            string libName = GetLibraryFileName();
            string rid = GetRuntimeIdentifier();
            string baseDir = AppContext.BaseDirectory;

            // Check runtimes/ next to assembly
            if (File.Exists(Path.Combine(baseDir, "runtimes", rid, "native", libName)))
            { _isAvailable = true; return true; }
            if (File.Exists(Path.Combine(baseDir, libName)))
            { _isAvailable = true; return true; }

            // Walk up to find monorepo dotnet/src/Databricks.Zerobus/runtimes/
            var dir = new DirectoryInfo(baseDir);
            while (dir?.Parent != null)
            {
                var runtimes = Path.Combine(dir.FullName, "src", "Databricks.Zerobus", "runtimes", rid, "native", libName);
                if (File.Exists(runtimes))
                { _isAvailable = true; return true; }
                dir = dir.Parent;
            }

            _isAvailable = false; return false;
        }
        catch { _isAvailable = false; return false; }
    }

    private static string GetLibraryFileName() =>
        RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "zerobus_ffi.dll"
        : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "libzerobus_ffi.dylib"
        : "libzerobus_ffi.so";

    private static string GetRuntimeIdentifier()
    {
        string os = RuntimeInformation.IsOSPlatform(OSPlatform.Windows) ? "win"
            : RuntimeInformation.IsOSPlatform(OSPlatform.OSX) ? "osx" : "linux";
        string arch = RuntimeInformation.ProcessArchitecture == Architecture.Arm64 ? "arm64" : "x64";
        return $"{os}-{arch}";
    }
}
