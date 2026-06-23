#include "test_harness.hpp"

// Runs every registered TEST and returns non-zero if any failed (CTest's
// pass/fail signal).
int main() { return zb_test::run_all(); }
