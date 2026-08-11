// UnackedRecord: owns its payload bytes, exposes the JSON/proto flag, and
// reinterprets the bytes via as_string(). Pure value type.

#include "zerobus/record.hpp"

#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

}  // namespace

int main() {
  using zerobus::UnackedRecord;

  // JSON record: flag set, bytes round-trip as vector and string.
  {
    const std::string json = R"({"id":7})";
    const std::vector<std::uint8_t> bytes(json.begin(), json.end());
    UnackedRecord rec(true, bytes);
    if (!rec.is_json()) {
      fail("JSON record should report is_json() == true");
    }
    if (rec.data() != bytes) {
      fail("data() did not preserve the payload bytes");
    }
    if (rec.as_string() != json) {
      fail("as_string() did not reconstruct the JSON payload");
    }
  }

  // Proto record: flag clear, binary bytes (incl. embedded NUL) preserved.
  {
    const std::vector<std::uint8_t> bytes = {0x00, 0x01, 0x02, 0x00, 0xff};
    UnackedRecord rec(false, bytes);
    if (rec.is_json()) {
      fail("proto record should report is_json() == false");
    }
    if (rec.data() != bytes) {
      fail("data() did not preserve binary proto bytes");
    }
    if (rec.data().size() != 5) {
      fail("data() truncated the payload at the embedded NUL");
    }
  }

  // Empty payload.
  {
    UnackedRecord rec(true, {});
    if (!rec.data().empty()) {
      fail("empty payload should yield empty data()");
    }
    if (!rec.as_string().empty()) {
      fail("empty payload should yield empty as_string()");
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("UnackedRecord preserves payload bytes and json/proto flag\n");
  return 0;
}
