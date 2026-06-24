import os
import re

from conan import ConanFile
from conan.errors import ConanException
from conan.tools.cmake import CMake, CMakeToolchain, cmake_layout
from conan.tools.files import copy, load, rmdir


class ZerobusSdkConan(ConanFile):
    """Conan recipe for the Zerobus C++ SDK.

    DRAFT / first cut. This builds the SDK and its Rust C FFI dependency from
    source (cargo), so a Rust toolchain must be on PATH at build time. The public
    publishing path (registry, ABI matrix, source-vs-prebuilt FFI) is still being
    settled with the secure-release platform team; expect this recipe to change.
    """

    name = "zerobus-sdk"
    package_type = "static-library"
    license = "Apache-2.0"
    homepage = "https://github.com/databricks/zerobus-sdk"
    url = "https://github.com/databricks/zerobus-sdk"
    description = (
        "C++17 SDK for high-throughput ingestion into Databricks Zerobus. "
        "A RAII wrapper over the Zerobus C FFI (Rust core)."
    )
    topics = ("databricks", "zerobus", "ingestion", "grpc", "arrow")

    settings = "os", "arch", "compiler", "build_type"
    options = {"fPIC": [True, False]}
    default_options = {"fPIC": True}

    def set_version(self):
        # Single source of truth: parse ZEROBUS_CPP_VERSION from version.hpp so
        # the package version stays in lockstep with the header and CMakeLists.
        header = os.path.join(self.recipe_folder, "include", "zerobus", "version.hpp")
        match = re.search(r'#define\s+ZEROBUS_CPP_VERSION\s+"([^"]+)"', load(self, header))
        if not match:
            raise ConanException("Could not parse ZEROBUS_CPP_VERSION from version.hpp")
        self.version = match.group(1)

    def export_sources(self):
        # The recipe lives in cpp/, but cmake/BuildRustFfi.cmake builds the FFI
        # from ../rust. Copy both trees so the exported source is self-contained
        # while preserving the cpp/ <-> rust/ sibling layout the CMake expects.
        repo = os.path.join(self.recipe_folder, "..")
        copy(self, "*",
             src=os.path.join(repo, "cpp"),
             dst=os.path.join(self.export_sources_folder, "cpp"),
             excludes=["build*/**", "test_package/**", "conanfile.py"])
        copy(self, "*",
             src=os.path.join(repo, "rust"),
             dst=os.path.join(self.export_sources_folder, "rust"),
             excludes=["target/**"])

    def config_options(self):
        if self.settings.os == "Windows":
            del self.options.fPIC

    def layout(self):
        # CMakeLists.txt is in the cpp/ subfolder of the exported source.
        cmake_layout(self, src_folder="cpp")

    def generate(self):
        tc = CMakeToolchain(self)
        tc.variables["ZEROBUS_BUILD_TESTS"] = "OFF"
        tc.variables["ZEROBUS_BUILD_EXAMPLES"] = "OFF"
        tc.generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()

    def package(self):
        cmake = CMake(self)
        cmake.install()
        # Let Conan's CMakeDeps regenerate the package config from package_info()
        # below; drop the SDK's own installed CMake config to avoid a clash.
        rmdir(self, os.path.join(self.package_folder, "lib", "cmake"))

    def package_info(self):
        # Consumers do find_package(zerobus) and link zerobus::zerobus.
        self.cpp_info.set_property("cmake_file_name", "zerobus")
        self.cpp_info.set_property("cmake_target_name", "zerobus::zerobus")
        # Link the C++ wrapper archive ahead of the FFI archive it depends on.
        self.cpp_info.libs = ["zerobus_cpp", "zerobus_ffi"]

        # System libraries the Rust static library needs at link time. Mirrors
        # cmake/BuildRustFfi.cmake.
        if self.settings.os == "Linux":
            self.cpp_info.system_libs = ["pthread", "dl", "m", "resolv"]
        elif self.settings.os == "Macos":
            self.cpp_info.frameworks = ["CoreFoundation", "Security"]
            self.cpp_info.system_libs = ["iconv"]
        elif self.settings.os == "Windows":
            self.cpp_info.system_libs = ["ws2_32", "userenv", "bcrypt", "ntdll"]
