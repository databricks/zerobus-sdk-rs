#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "error.h"
#include "zerobus/error.h"

static char *format_message(const char *fmt, va_list ap, size_t *out_len)
{
    va_list ap2;
    va_copy(ap2, ap);
    int needed = vsnprintf(NULL, 0, fmt, ap2);
    va_end(ap2);
    if (needed < 0) {
        *out_len = 0;
        return NULL;
    }
    char *buf = (char *)malloc((size_t)needed + 1);
    if (buf == NULL) {
        *out_len = 0;
        return NULL;
    }
    vsnprintf(buf, (size_t)needed + 1, fmt, ap);
    *out_len = (size_t)needed;
    return buf;
}

/*
 * Shared body taking a va_list, so both public constructors format the message
 * once.
 */
static zerobus_error_t *error_vnewf(zerobus_status_t code, const char *fmt,
                                    va_list ap)
{
    zerobus_error_t *err = (zerobus_error_t *)calloc(1, sizeof(*err));
    if (err == NULL) {
        return NULL;
    }
    err->code = code;
    err->message = format_message(fmt, ap, &err->message_len);
    if (err->message == NULL) {
        free(err);
        return NULL;
    }
    return err;
}

zerobus_error_t *zb_error_newf(zerobus_status_t code, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    zerobus_error_t *err = error_vnewf(code, fmt, ap);
    va_end(ap);
    return err;
}

zerobus_status_t zb_fail(zerobus_error_t **out_error, zerobus_status_t code,
                         const char *fmt, ...)
{
    if (out_error != NULL) {
        va_list ap;
        va_start(ap, fmt);
        *out_error = error_vnewf(code, fmt, ap);
        va_end(ap);
    }
    return code;
}

zerobus_string_view_t zerobus_error_message(const zerobus_error_t *error)
{
    if (error == NULL || error->message == NULL) {
        return (zerobus_string_view_t){NULL, 0};
    }
    return (zerobus_string_view_t){error->message, error->message_len};
}

void zerobus_error_free(zerobus_error_t *error)
{
    if (error == NULL) {
        return;
    }
    free(error->message);
    free(error);
}
