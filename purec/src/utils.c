#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "utils.h"

/* ---- UTF-8 -------------------------------------------------------------- */

ZB_STATIC bool is_valid_utf8(const char *text, size_t len)
{
    /* Byte inspection below needs unsigned arithmetic: char is signed on some
     * platforms, which would break the lead/continuation-byte masks. */
    const uint8_t *bytes = (const uint8_t *)text;
    if (bytes == NULL) {
        return len == 0;
    }
    for (size_t i = 0; i < len; i++) {
        uint8_t c = bytes[i];
        if (c == 0x00) {
            return false; /* embedded NUL */
        }
        if (c < 0x80) {
            continue;
        }
        size_t extra;
        uint8_t lo, hi;
        if ((c & 0xE0) == 0xC0) {
            extra = 1;
            lo = 0x80;
            hi = 0xBF;
            if (c < 0xC2) {
                return false; /* overlong 2-byte */
            }
        } else if ((c & 0xF0) == 0xE0) {
            extra = 2;
            /* Constrain the first continuation byte to reject overlong forms
             * and UTF-16 surrogates. */
            lo = (c == 0xE0) ? 0xA0 : 0x80;
            hi = (c == 0xED) ? 0x9F : 0xBF;
        } else if ((c & 0xF8) == 0xF0) {
            extra = 3;
            lo = (c == 0xF0) ? 0x90 : 0x80;
            hi = (c == 0xF4) ? 0x8F : 0xBF;
            if (c > 0xF4) {
                return false; /* > U+10FFFF */
            }
        } else {
            return false; /* invalid lead byte or stray continuation */
        }
        if (i + extra >= len) {
            return false; /* truncated sequence */
        }
        uint8_t first = bytes[i + 1];
        if (first < lo || first > hi) {
            return false;
        }
        for (size_t k = 2; k <= extra; ++k) {
            uint8_t cc = bytes[i + k];
            if ((cc & 0xC0) != 0x80) {
                return false;
            }
        }
        i += extra; /* the loop's i++ accounts for the lead byte */
    }
    return true;
}

/* ---- minimal JSON structural validator --------------------------------- */

typedef struct {
    const uint8_t *p;
    const uint8_t *end;
} json_reader;

static void json_skip_ws(json_reader *r)
{
    while (r->p < r->end) {
        uint8_t c = *r->p;
        if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
            r->p++;
        } else {
            break;
        }
    }
}

static bool json_value(json_reader *r, int depth);

static bool json_literal(json_reader *r, const char *lit)
{
    size_t n = strlen(lit);
    if ((size_t)(r->end - r->p) < n || memcmp(r->p, lit, n) != 0) {
        return false;
    }
    r->p += n;
    return true;
}

static bool json_string(json_reader *r)
{
    if (r->p >= r->end || *r->p != '"') {
        return false;
    }
    r->p++;
    while (r->p < r->end) {
        uint8_t c = *r->p++;
        if (c == '"') {
            return true;
        }
        if (c == '\\') {
            if (r->p >= r->end) {
                return false;
            }
            uint8_t e = *r->p++;
            if (e == 'u') {
                if (r->end - r->p < 4) {
                    return false;
                }
                for (int i = 0; i < 4; ++i) {
                    uint8_t h = *r->p++;
                    bool hex = (h >= '0' && h <= '9') ||
                               (h >= 'a' && h <= 'f') || (h >= 'A' && h <= 'F');
                    if (!hex) {
                        return false;
                    }
                }
            } else if (e != '"' && e != '\\' && e != '/' && e != 'b' &&
                       e != 'f' && e != 'n' && e != 'r' && e != 't') {
                return false;
            }
        } else if (c < 0x20) {
            return false; /* control characters must be escaped */
        }
    }
    return false; /* unterminated */
}

static bool json_number(json_reader *r)
{
    const uint8_t *start = r->p;
    if (r->p < r->end && *r->p == '-') {
        r->p++;
    }
    if (r->p >= r->end) {
        return false;
    }
    if (*r->p == '0') {
        r->p++;
    } else if (*r->p >= '1' && *r->p <= '9') {
        while (r->p < r->end && *r->p >= '0' && *r->p <= '9') {
            r->p++;
        }
    } else {
        return false;
    }
    if (r->p < r->end && *r->p == '.') {
        r->p++;
        if (r->p >= r->end || *r->p < '0' || *r->p > '9') {
            return false;
        }
        while (r->p < r->end && *r->p >= '0' && *r->p <= '9') {
            r->p++;
        }
    }
    if (r->p < r->end && (*r->p == 'e' || *r->p == 'E')) {
        r->p++;
        if (r->p < r->end && (*r->p == '+' || *r->p == '-')) {
            r->p++;
        }
        if (r->p >= r->end || *r->p < '0' || *r->p > '9') {
            return false;
        }
        while (r->p < r->end && *r->p >= '0' && *r->p <= '9') {
            r->p++;
        }
    }
    return r->p != start;
}

static bool json_array(json_reader *r, int depth)
{
    if (depth > ZB_JSON_MAX_DEPTH) {
        return false;
    }
    r->p++; /* consume '[' */
    json_skip_ws(r);
    if (r->p < r->end && *r->p == ']') {
        r->p++;
        return true;
    }
    for (;;) {
        if (!json_value(r, depth)) {
            return false;
        }
        json_skip_ws(r);
        if (r->p >= r->end) {
            return false;
        }
        if (*r->p == ',') {
            r->p++;
            continue;
        }
        if (*r->p == ']') {
            r->p++;
            return true;
        }
        return false;
    }
}

static bool json_object(json_reader *r, int depth)
{
    if (depth > ZB_JSON_MAX_DEPTH) {
        return false;
    }
    r->p++; /* consume '{' */
    json_skip_ws(r);
    if (r->p < r->end && *r->p == '}') {
        r->p++;
        return true;
    }
    for (;;) {
        json_skip_ws(r);
        if (!json_string(r)) {
            return false;
        }
        json_skip_ws(r);
        if (r->p >= r->end || *r->p != ':') {
            return false;
        }
        r->p++;
        json_skip_ws(r);
        if (!json_value(r, depth)) {
            return false;
        }
        json_skip_ws(r);
        if (r->p >= r->end) {
            return false;
        }
        if (*r->p == ',') {
            r->p++;
            continue;
        }
        if (*r->p == '}') {
            r->p++;
            return true;
        }
        return false;
    }
}

static bool json_value(json_reader *r, int depth)
{
    json_skip_ws(r);
    if (r->p >= r->end) {
        return false;
    }
    switch (*r->p) {
    case '{':
        return json_object(r, depth + 1);
    case '[':
        return json_array(r, depth + 1);
    case '"':
        return json_string(r);
    case 't':
        return json_literal(r, "true");
    case 'f':
        return json_literal(r, "false");
    case 'n':
        return json_literal(r, "null");
    default:
        return json_number(r);
    }
}

ZB_STATIC bool is_valid_json(const char *text, size_t len)
{
    if (text == NULL || len == 0) {
        return false;
    }
    if (!is_valid_utf8(text, len)) {
        return false;
    }
    const uint8_t *bytes = (const uint8_t *)text;
    json_reader r = {bytes, bytes + len};
    if (!json_value(&r, 0)) {
        return false;
    }
    json_skip_ws(&r);
    return r.p == r.end; /* nothing but whitespace may trail the value */
}

/* ---- view classification ----------------------------------------------- */

bool zb_is_empty(zerobus_string_view_t view)
{
    return view.data == NULL || view.len == 0;
}

bool zb_is_valid_string(zerobus_string_view_t view)
{
    if (zb_is_empty(view)) {
        return false;
    }
    return is_valid_utf8(view.data, view.len);
}

bool zb_is_valid_json(zerobus_string_view_t view)
{
    if (zb_is_empty(view)) {
        return false;
    }
    return is_valid_json(view.data, view.len);
}

/* ---- table name -------------------------------------------------------- */

bool zb_table_name_is_valid(zerobus_string_view_t table)
{
    if (!zb_is_valid_string(table)) {
        return false;
    }
    /* Exactly three non-empty, dot-separated components. */
    size_t component_len = 0;
    size_t components = 0;
    for (size_t i = 0; i < table.len; ++i) {
        if (table.data[i] == '.') {
            if (component_len == 0) {
                return false; /* empty component (leading, trailing, or "..") */
            }
            components++;
            component_len = 0;
        } else {
            component_len++;
        }
    }
    if (component_len == 0) {
        return false; /* trailing dot */
    }
    components++;
    return components == 3;
}

/* ---- endpoint URL ------------------------------------------------------- */

/* Non-empty dot-separated labels, with one optional trailing dot (the DNS root
 * form). Rejects "a..b", ".a" and "". */
ZB_STATIC bool host_labels_are_valid(const char *host, size_t len)
{
    size_t label = 0;
    for (size_t i = 0; i < len; ++i) {
        if (host[i] == '.') {
            if (label == 0) {
                return false;
            }
            label = 0;
        } else {
            label++;
        }
    }
    /* Empty labels (leading or interior) are already rejected in the loop, so
     * reaching here with a non-empty host means every label is valid. */
    return len > 0;
}

zb_url_result zb_url_validate(const char *endpoint, char **out_host,
                              bool *out_is_https)
{
    if (out_host != NULL) {
        *out_host = NULL;
    }
    if (endpoint == NULL) {
        return ZB_URL_NO_SCHEME;
    }

    const char *rest;
    bool is_https;
    if (strncmp(endpoint, "https://", 8) == 0) {
        rest = endpoint + 8;
        is_https = true;
    } else if (strncmp(endpoint, "http://", 7) == 0) {
        rest = endpoint + 7;
        is_https = false;
    } else {
        return ZB_URL_NO_SCHEME;
    }

    /* The authority ends at the first '/', '?' or '#'. */
    size_t authority_len = 0;
    while (rest[authority_len] != '\0' && rest[authority_len] != '/' &&
           rest[authority_len] != '?' && rest[authority_len] != '#') {
        ++authority_len;
    }
    if (authority_len == 0) {
        return ZB_URL_EMPTY_HOST;
    }

    /* Userinfo ("user[:pass]@host") is not supported. */
    for (size_t i = 0; i < authority_len; ++i) {
        if (rest[i] == '@') {
            return ZB_URL_BAD_HOST;
        }
    }

    /* The host is the authority up to an optional ":port". IPv6 literals in
     * [brackets] carry their own colons and are not supported. */
    size_t host_len = authority_len;
    for (size_t i = 0; i < authority_len; ++i) {
        if (rest[i] == ':') {
            host_len = i;
            break;
        }
    }
    if (host_len == 0) {
        return ZB_URL_EMPTY_HOST;
    }
    if (rest[0] == '[') {
        return ZB_URL_BAD_HOST;
    }
    for (size_t i = 0; i < host_len; ++i) {
        unsigned char c = (unsigned char)rest[i];
        if (c <= ' ' || c == 0x7f) { /* no spaces or control characters */
            return ZB_URL_BAD_HOST;
        }
    }
    if (!host_labels_are_valid(rest, host_len)) {
        return ZB_URL_BAD_HOST;
    }

    /* A present port must be a non-empty run of digits in 1..65535. */
    if (host_len < authority_len) {
        if (host_len + 1 == authority_len) { /* ':' with nothing after it */
            return ZB_URL_BAD_PORT;
        }
        unsigned long port = 0;
        for (size_t i = host_len + 1; i < authority_len; ++i) {
            if (rest[i] < '0' || rest[i] > '9') {
                return ZB_URL_BAD_PORT;
            }
            port = port * 10u + (unsigned long)(rest[i] - '0');
            if (port > 65535u) {
                return ZB_URL_BAD_PORT;
            }
        }
        if (port == 0) {
            return ZB_URL_BAD_PORT;
        }
    }

    /* Both out-params are written only on success, so a failure leaves them as
     * the caller set them (*out_host was NULL'd on entry). */
    if (out_host != NULL) {
        char *host = zb_strndup(rest, host_len);
        if (host == NULL) {
            return ZB_URL_OOM;
        }
        *out_host = host;
    }
    if (out_is_https != NULL) {
        *out_is_https = is_https;
    }
    return ZB_URL_OK;
}

/* ---- owned-memory helpers ---------------------------------------------- */

bool zb_replace_string(char **target, zerobus_string_view_t view)
{
    char *copy = zb_strdup_view(view);
    if (copy == NULL) {
        return false;
    }
    free(*target);
    *target = copy;
    return true;
}

char *zb_strndup(const char *src, size_t len)
{
    if (src == NULL) {
        return NULL;
    }
    char *out = (char *)malloc(len + 1);
    if (out == NULL) {
        return NULL;
    }
    memcpy(out, src, len);
    out[len] = '\0';
    return out;
}

char *zb_strdup_view(zerobus_string_view_t view)
{
    return zb_strndup(view.data, view.len);
}

char *zb_strdup(const char *s)
{
    if (s == NULL) {
        return NULL;
    }
    return zb_strndup(s, strlen(s));
}

/*
 * A volatile pointer stops a compiler from treating the store as dead. It is
 * the plain-C stand-in for the platform explicit_bzero / SecureZeroMemory the
 * production layer would use.
 */
void zb_secure_zero(void *p, size_t len)
{
    if (p == NULL || len == 0) {
        return;
    }
    volatile unsigned char *q = (volatile unsigned char *)p;
    while (len-- > 0) {
        *q++ = 0;
    }
}

void zb_secure_free(void *p, size_t len)
{
    if (p == NULL) {
        return;
    }
    zb_secure_zero(p, len);
    free(p);
}

void zb_secure_free_cstr(char *s)
{
    zb_secure_free(s, s != NULL ? strlen(s) : 0);
}
