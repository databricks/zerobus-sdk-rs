/*
 * Zerobus Pure C SDK — shared declarations for the public headers.
 *
 * Export macros, opaque handle typedefs, view types, and status codes. Every
 * module header includes this file. C99, with C++ compatibility via extern "C".
 */
#ifndef ZEROBUS_COMMON_H
#define ZEROBUS_COMMON_H

#include <stddef.h>
#include <stdint.h>

/*
 * ZEROBUS_API decorates the public symbols and ZEROBUS_CALL pins their calling
 * convention. We ship only a static archive, which needs no export decoration
 * yet — a future shared or DLL build will add it.
 */
#ifndef ZEROBUS_API
#if defined(__GNUC__) && __GNUC__ >= 4
#define ZEROBUS_API __attribute__((visibility("default")))
#else
#define ZEROBUS_API
#endif
#endif

#ifndef ZEROBUS_CALL
#if defined(_WIN32) && !defined(_WIN64)
#define ZEROBUS_CALL __cdecl
#else
#define ZEROBUS_CALL
#endif
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct zerobus_sdk_builder zerobus_sdk_builder_t;
typedef struct zerobus_sdk zerobus_sdk_t;
typedef struct zerobus_stream_builder zerobus_stream_builder_t;
typedef struct zerobus_stream zerobus_stream_t;
typedef struct zerobus_error zerobus_error_t;

/*
 * A borrowed view of UTF-8 text: the caller must pass valid UTF-8 with no
 * embedded NUL. It need not be NUL-terminated, and is read only for the
 * duration of the call it is passed to. {NULL, 0} is empty. {NULL, nonzero} is
 * invalid.
 */
typedef struct zerobus_string_view {
    const char *data;
    size_t len;
} zerobus_string_view_t;

/* C uses a compound literal. C++ (which has none) uses a braced temporary. */
#ifdef __cplusplus
#define ZEROBUS_STRING_LITERAL(value)                                          \
    (zerobus_string_view_t{(value), sizeof(value) - 1u})
#else
#define ZEROBUS_STRING_LITERAL(value)                                          \
    ((zerobus_string_view_t){(value), sizeof(value) - 1u})
#endif

/*
 * Status codes have a fixed-width ABI. Values are explicit and are never
 * reordered or reused.
 */
typedef uint32_t zerobus_status_t;

enum {
    ZEROBUS_STATUS_OK = 0,
    ZEROBUS_STATUS_UNKNOWN = 1,
    ZEROBUS_STATUS_INVALID_ARGUMENT = 2,
    ZEROBUS_STATUS_DEADLINE_EXCEEDED = 3,
    ZEROBUS_STATUS_CANCELLED = 4,
    ZEROBUS_STATUS_FAILED_PRECONDITION = 5,
    ZEROBUS_STATUS_RESOURCE_EXHAUSTED = 6,
    ZEROBUS_STATUS_UNAUTHENTICATED = 7,
    ZEROBUS_STATUS_PERMISSION_DENIED = 8,
    ZEROBUS_STATUS_NOT_FOUND = 9,
    ZEROBUS_STATUS_UNAVAILABLE = 10,
    ZEROBUS_STATUS_UNIMPLEMENTED = 11,
    ZEROBUS_STATUS_OUT_OF_MEMORY = 12,
    ZEROBUS_STATUS_INTERNAL = 13
};

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* ZEROBUS_COMMON_H */
