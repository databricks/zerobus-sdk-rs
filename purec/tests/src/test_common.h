/*
 * Shared support for the offline unit tests (no external framework): assertion
 * macros, the failure counter, and helpers common to more than one test file.
 */
#ifndef ZB_TEST_COMMON_H
#define ZB_TEST_COMMON_H

#include <stdio.h> // IWYU pragma: keep (fprintf, used in the CHECK macros below)
#include <string.h>

#include "zerobus/zerobus.h"

static int zb_test_failures = 0;

#define CHECK(cond)                                                            \
    do {                                                                       \
        if (!(cond)) {                                                         \
            fprintf(stderr, "  FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
            zb_test_failures++;                                                \
        }                                                                      \
    } while (0)

#define CHECK_EQ_INT(a, b)                                                     \
    do {                                                                       \
        long long _a = (long long)(a), _b = (long long)(b);                    \
        if (_a != _b) {                                                        \
            fprintf(stderr, "  FAIL %s:%d: %s (%lld) != %s (%lld)\n",          \
                    __FILE__, __LINE__, #a, _a, #b, _b);                       \
            zb_test_failures++;                                                \
        }                                                                      \
    } while (0)

#define TEST_MAIN_RETURN() return zb_test_failures == 0 ? 0 : 1

/* Build a string view from a NUL-terminated C string. */
static inline zerobus_string_view_t sv(const char *s)
{
    return (zerobus_string_view_t){s, strlen(s)};
}

#endif /* ZB_TEST_COMMON_H */
