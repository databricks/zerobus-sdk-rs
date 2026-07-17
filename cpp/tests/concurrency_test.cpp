// Concurrent-liveness / consistency smoke test for the documented contract that
// a single ProtoSchema supports concurrent readers (descriptor_bytes /
// encode_json are const). Run under ThreadSanitizer (`make test
// SANITIZE=thread`), but note TSan only instruments the C++ side: the work runs
// in the Rust FFI, which is built without sanitizer RUSTFLAGS, so this asserts
// consistent results across threads rather than detecting a Rust-side race.
// Hermetic, so it runs in CI. No concurrent-Stream case: a Stream is not safe
// for concurrent use, so that would test misuse.

#include <cstddef>
#include <cstdio>
#include <thread>
#include <vector>

#include "zerobus/error.hpp"
#include "zerobus/proto_schema.hpp"

namespace {

int g_failures = 0;

void fail(const char* msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg);
  ++g_failures;
}

const char* kUcTableJson = R"({
  "name": "events",
  "catalog_name": "main",
  "schema_name": "analytics",
  "columns": [
    {"name": "id", "type_name": "BIGINT", "type_text": "bigint", "nullable": false, "position": 0},
    {"name": "payload", "type_name": "STRING", "type_text": "string", "nullable": true, "position": 1}
  ]
})";

}  // namespace

int main() {
  using zerobus::ProtoSchema;
  using zerobus::ZerobusException;

  ProtoSchema schema = ProtoSchema::from_uc_json(kUcTableJson);
  const std::size_t expected_descriptor_len = schema.descriptor_bytes().size();
  if (expected_descriptor_len == 0) {
    fail("schema fixture produced an empty descriptor");
  }

  // Each thread repeatedly reads the descriptor and encodes a record on the
  // shared schema; results must stay consistent and race-free.
  constexpr int kThreads = 8;
  constexpr int kIterations = 200;

  std::vector<int> per_thread_failures(kThreads, 0);
  std::vector<std::thread> threads;
  threads.reserve(kThreads);

  for (int t = 0; t < kThreads; ++t) {
    threads.emplace_back([&, t]() {
      for (int i = 0; i < kIterations; ++i) {
        try {
          if (schema.descriptor_bytes().size() != expected_descriptor_len) {
            ++per_thread_failures[t];
          }
          if (schema.encode_json(R"({"id": 1, "payload": "x"})").empty()) {
            ++per_thread_failures[t];
          }
        } catch (const ZerobusException&) {
          ++per_thread_failures[t];
        }
      }
    });
  }
  for (std::thread& th : threads) {
    th.join();
  }

  for (int t = 0; t < kThreads; ++t) {
    if (per_thread_failures[t] != 0) {
      fail("a concurrent reader observed an inconsistent or failed result");
      break;
    }
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("%d threads x %d reads on a shared ProtoSchema: consistent\n",
              kThreads, kIterations);
  return 0;
}
