/*
 * Internal error construction, shared by every public entry point.
 */
#ifndef ZB_ERROR_H
#define ZB_ERROR_H

#include "zerobus/common.h"

struct zerobus_error {
    zerobus_status_t code;
    char *message;
    size_t message_len;
};

/*
 * Build an owned error with a printf-formatted message. Returns NULL only if
 * allocating the error itself fails, which callers surface as
 * ZEROBUS_STATUS_OUT_OF_MEMORY with *out_error == NULL.
 */
zerobus_error_t *zb_error_newf(zerobus_status_t code, const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 2, 3)))
#endif
    ;

/*
 * Convenience for the common "set *out_error (if non-NULL) and return status"
 * pattern used by every public entry point. When error allocation fails, leaves
 * *out_error as NULL. Always returns `code`.
 */
zerobus_status_t zb_fail(zerobus_error_t **out_error, zerobus_status_t code,
                         const char *fmt, ...)
#if defined(__GNUC__)
    __attribute__((format(printf, 3, 4)))
#endif
    ;

#endif /* ZB_ERROR_H */
