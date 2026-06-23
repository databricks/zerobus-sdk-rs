#ifndef ZEROBUS_TEST_HARNESS_HPP
#define ZEROBUS_TEST_HARNESS_HPP

// A tiny, dependency-free test harness for the Zerobus C++ SDK.
//
// It implements just the slice of the GoogleTest API these tests use — TEST,
// EXPECT_*/ASSERT_* comparisons, EXPECT_THROW, FAIL, SUCCEED — so the suite
// stays hermetic (no vendored framework, no package manager, no network). The
// macro shape mirrors GoogleTest's `AssertHelper = Message()` trick so that a
// trailing `<< "context"` works and ASSERT_*/FAIL can `return` from the test.
//
// Failures print `file:line: <expr>` and mark the current test failed; the
// process exits non-zero if any test fails, which is what CTest checks.

#include <cstring>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

namespace zb_test {

struct TestCase {
  const char* suite;
  const char* name;
  void (*fn)();
};

// Shared across translation units via inline-function-local statics.
inline std::vector<TestCase>& registry() {
  static std::vector<TestCase> r;
  return r;
}
inline bool& current_failed() {
  static bool failed = false;
  return failed;
}

inline int register_test(const char* suite, const char* name, void (*fn)()) {
  registry().push_back(TestCase{suite, name, fn});
  return 0;
}

// Accumulates an optional failure message streamed after an assertion macro.
class Message {
 public:
  template <class T>
  Message& operator<<(const T& value) {
    os_ << value;
    return *this;
  }
  std::string str() const { return os_.str(); }

 private:
  std::ostringstream os_;
};

// Records a failure when assigned a Message (the assignment is the sink so that
// `<< "context"` binds to the Message before operator= runs, per precedence).
class AssertHelper {
 public:
  AssertHelper(const char* file, int line, const char* expr)
      : file_(file), line_(line), expr_(expr) {}

  void operator=(const Message& message) const {
    current_failed() = true;
    std::cerr << file_ << ":" << line_ << ": Failure: " << expr_;
    const std::string extra = message.str();
    if (!extra.empty()) {
      std::cerr << " — " << extra;
    }
    std::cerr << "\n";
  }

 private:
  const char* file_;
  int line_;
  const char* expr_;
};

template <class A, class B>
bool eq(const A& a, const B& b) {
  return a == b;
}

inline bool streq(const char* a, const char* b) {
  if (a == b) return true;
  if (a == nullptr || b == nullptr) return false;
  return std::strcmp(a, b) == 0;
}

template <class Ex, class F>
bool throws(F&& f) {
  try {
    f();
  } catch (const Ex&) {
    return true;
  } catch (...) {
    return false;
  }
  return false;
}

inline int run_all() {
  int passed = 0;
  int failed = 0;
  for (const TestCase& tc : registry()) {
    current_failed() = false;
    std::cerr << "[ RUN      ] " << tc.suite << "." << tc.name << "\n";
    try {
      tc.fn();
    } catch (const std::exception& e) {
      current_failed() = true;
      std::cerr << "  uncaught std::exception: " << e.what() << "\n";
    } catch (...) {
      current_failed() = true;
      std::cerr << "  uncaught non-std exception\n";
    }
    if (current_failed()) {
      ++failed;
      std::cerr << "[  FAILED  ] " << tc.suite << "." << tc.name << "\n";
    } else {
      ++passed;
      std::cerr << "[       OK ] " << tc.suite << "." << tc.name << "\n";
    }
  }
  std::cerr << "\n"
            << passed << " passed, " << failed << " failed, "
            << (passed + failed) << " total\n";
  return failed == 0 ? 0 : 1;
}

}  // namespace zb_test

// Avoids the dangling-else ambiguity when a macro is used in an unbraced
// if/else, exactly as GoogleTest's GTEST_AMBIGUOUS_ELSE_BLOCKER_ does.
#define ZB_AMBIGUOUS_ELSE_BLOCKER_ \
  switch (0)                       \
  case 0:                          \
  default:  // NOLINT

#define ZB_NONFATAL_(cond, expr_text)                        \
  ZB_AMBIGUOUS_ELSE_BLOCKER_                                 \
  if (cond) {                                                \
  } else                                                     \
    ::zb_test::AssertHelper(__FILE__, __LINE__, expr_text) = \
        ::zb_test::Message()

#define ZB_FATAL_(cond, expr_text)                                  \
  ZB_AMBIGUOUS_ELSE_BLOCKER_                                        \
  if (cond) {                                                       \
  } else                                                            \
    return ::zb_test::AssertHelper(__FILE__, __LINE__, expr_text) = \
               ::zb_test::Message()

#define TEST(suite, name)                                                 \
  static void zb_test_##suite##_##name();                                 \
  [[maybe_unused]] static const int zb_reg_##suite##_##name =             \
      ::zb_test::register_test(#suite, #name, &zb_test_##suite##_##name); \
  static void zb_test_##suite##_##name()

#define EXPECT_TRUE(c) ZB_NONFATAL_((c), "EXPECT_TRUE(" #c ")")
#define EXPECT_FALSE(c) ZB_NONFATAL_(!(c), "EXPECT_FALSE(" #c ")")
#define EXPECT_EQ(a, b) \
  ZB_NONFATAL_(::zb_test::eq((a), (b)), "EXPECT_EQ(" #a ", " #b ")")
#define EXPECT_NE(a, b) \
  ZB_NONFATAL_(!::zb_test::eq((a), (b)), "EXPECT_NE(" #a ", " #b ")")
#define EXPECT_STREQ(a, b) \
  ZB_NONFATAL_(::zb_test::streq((a), (b)), "EXPECT_STREQ(" #a ", " #b ")")

#define ASSERT_EQ(a, b) \
  ZB_FATAL_(::zb_test::eq((a), (b)), "ASSERT_EQ(" #a ", " #b ")")
#define ASSERT_NE(a, b) \
  ZB_FATAL_(!::zb_test::eq((a), (b)), "ASSERT_NE(" #a ", " #b ")")

#define EXPECT_THROW(stmt, ex)                         \
  ZB_NONFATAL_((::zb_test::throws<ex>([&] { stmt; })), \
               "EXPECT_THROW(" #stmt ", " #ex ")")

#define FAIL()                                                   \
  return ::zb_test::AssertHelper(__FILE__, __LINE__, "FAIL()") = \
             ::zb_test::Message()
#define SUCCEED() (void)0

#endif  // ZEROBUS_TEST_HARNESS_HPP
