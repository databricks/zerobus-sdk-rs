// Exercises detail::make_proto_batch, which both ingest_proto_records overloads
// route through.
//
// The ingest call itself needs a live server, but the invariants here do not:
// the arrays must ALIAS the caller's bytes (asserted by pointer identity — a
// regression to copying would still behave correctly, so nothing else catches
// it), the FFI must never get a null payload pointer, and the two overloads
// must agree.

#include "detail/proto_batch.hpp"

#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <string>
#include <vector>

#include "zerobus/error.hpp"
#include "zerobus/record.hpp"

namespace {

using zerobus::ProtoRecordView;
using zerobus::ZerobusException;
using zerobus::detail::make_proto_batch;
using zerobus::detail::ProtoBatchView;

int g_failures = 0;

void fail(const std::string& msg) {
  std::fprintf(stderr, "FAIL: %s\n", msg.c_str());
  ++g_failures;
}

void expect(bool condition, const std::string& msg) {
  if (!condition) {
    fail(msg);
  }
}

// Includes an empty record: the case where a naive data() hands the FFI null.
std::vector<std::vector<std::uint8_t>> sample_records() {
  return {{1, 2, 3}, {42}, {}};
}

std::vector<ProtoRecordView> views_of(
    const std::vector<std::vector<std::uint8_t>>& records) {
  std::vector<ProtoRecordView> views;
  views.reserve(records.size());
  for (const auto& r : records) {
    views.push_back({r.data(), r.size()});
  }
  return views;
}

// Lengths mirror the inputs; non-empty records are aliased, not copied.
void check_matches_records(
    const ProtoBatchView& v,
    const std::vector<std::vector<std::uint8_t>>& records,
    const std::string& who) {
  expect(v.ptrs.size() == records.size(), who + ": wrong pointer count");
  expect(v.lens.size() == records.size(), who + ": wrong length count");
  if (v.ptrs.size() != records.size() || v.lens.size() != records.size()) {
    return;  // Indexing below would be out of bounds.
  }
  for (std::size_t i = 0; i < records.size(); ++i) {
    expect(v.lens[i] == records[i].size(),
           who + ": length mismatch at index " + std::to_string(i));
    expect(
        v.ptrs[i] != nullptr,
        who + ": null pointer handed to the FFI at index " + std::to_string(i));
    if (!records[i].empty()) {
      expect(v.ptrs[i] == records[i].data(),
             who + ": record " + std::to_string(i) +
                 " was copied instead of aliased");
    }
  }
}

}  // namespace

int main() {
  const std::vector<std::vector<std::uint8_t>> records = sample_records();

  const ProtoBatchView from_vector = make_proto_batch(records);
  check_matches_records(from_vector, records, "vector overload");

  const std::vector<ProtoRecordView> views = views_of(records);
  const ProtoBatchView from_views =
      make_proto_batch(views.data(), views.size());
  check_matches_records(from_views, records, "borrowing overload");

  // Interchangeable, down to the sentinel used for the empty record.
  expect(from_vector.ptrs == from_views.ptrs,
         "overloads disagree on the pointer array");
  expect(from_vector.lens == from_views.lens,
         "overloads disagree on the length array");

  // A default-constructed view is {nullptr, 0}: a valid empty record, which
  // still reaches the FFI as a non-null pointer.
  {
    const ProtoRecordView empty{};
    const ProtoBatchView v = make_proto_batch(&empty, 1);
    expect(v.ptrs.size() == 1 && v.lens.size() == 1,
           "{nullptr, 0} did not produce a single-record view");
    if (v.ptrs.size() == 1 && v.lens.size() == 1) {
      expect(v.ptrs[0] != nullptr,
             "{nullptr, 0} passed a null pointer to the FFI");
      expect(v.lens[0] == 0, "{nullptr, 0} did not produce a zero length");
    }
  }

  // A zero-count batch must not touch the array at all, even a null one.
  {
    bool threw = false;
    try {
      const ProtoBatchView v = make_proto_batch(nullptr, 0);
      expect(v.ptrs.empty() && v.lens.empty(),
             "zero-count batch produced a non-empty view");
    } catch (const ZerobusException&) {
      threw = true;
    }
    expect(!threw, "zero-count batch with a null array was rejected");
  }

  // A null array with records to read must be reported, not dereferenced.
  {
    bool threw = false;
    try {
      make_proto_batch(nullptr, 3);
    } catch (const ZerobusException&) {
      threw = true;
    }
    expect(threw, "null record array with a non-zero count was NOT rejected");
  }

  // Likewise a null payload claiming a non-zero size: the core would read it.
  {
    const std::vector<ProtoRecordView> bad = {
        {records[0].data(), records[0].size()},
        {nullptr, 7},
    };
    bool threw = false;
    try {
      make_proto_batch(bad.data(), bad.size());
    } catch (const ZerobusException& e) {
      threw = true;
      expect(std::string(e.what()).find("index 1") != std::string::npos,
             "exception message did not name the offending record index");
    }
    expect(threw, "null payload with a non-zero size was NOT rejected");
  }

  if (g_failures != 0) {
    std::fprintf(stderr, "%d check(s) failed.\n", g_failures);
    return 1;
  }
  std::printf("proto batch adaptation aliases caller bytes and guards nulls\n");
  return 0;
}
