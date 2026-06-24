// Smoke test: include the umbrella header and exercise a symbol that forces the
// FFI archive to link. No network or credentials are used, so this is safe to
// run on any build machine.
#include <iostream>

#include "zerobus/zerobus.hpp"

int main() {
  std::cout << "Zerobus C++ SDK version: " << zerobus::version() << "\n";
  return 0;
}
