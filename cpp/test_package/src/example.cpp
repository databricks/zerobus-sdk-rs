// Smoke test for the packaged SDK. Constructing an SdkBuilder is a real call
// into the SDK (and through it, the Rust C FFI), so the link genuinely pulls in
// both the zerobus_cpp and zerobus_ffi archives — this proves the package is
// linkable and runnable, not merely discoverable by find_package. No network or
// credentials are used: the builder is created and immediately destroyed.
#include <iostream>

#include "zerobus/zerobus.hpp"

int main() {
  zerobus::SdkBuilder builder = zerobus::Sdk::builder();
  std::cout << "Zerobus C++ SDK version: " << zerobus::version() << "\n";
  return 0;
}
