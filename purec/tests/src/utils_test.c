/* Unit tests for the input-validation and owned-string helpers in utils.c. */
#include <stdlib.h>
#include <string.h>

#include "test_common.h"
#include "utils.h"

/* ---- helpers ----------------------------------------------------------- */

static bool utf8_ok(const char *s)
{
    return is_valid_utf8(s, strlen(s));
}

static bool json_ok(const char *s)
{
    return is_valid_json(s, strlen(s));
}

/* Wrap "0" in `depth` levels of `open`/`close` and report whether it validates,
 * e.g. nested_ok("[", "]", 3) checks "[[[0]]]". */
static bool nested_ok(const char *open, const char *close, size_t depth)
{
    size_t ol = strlen(open), cl = strlen(close);
    char *buf = (char *)malloc(depth * ol + 1 + depth * cl);
    if (buf == NULL) {
        return false;
    }
    char *p = buf;
    for (size_t i = 0; i < depth; ++i) {
        memcpy(p, open, ol);
        p += ol;
    }
    *p++ = '0';
    for (size_t i = 0; i < depth; ++i) {
        memcpy(p, close, cl);
        p += cl;
    }
    bool ok = is_valid_json(buf, (size_t)(p - buf));
    free(buf);
    return ok;
}

static bool labels_ok(const char *s)
{
    return host_labels_are_valid(s, strlen(s));
}

/* ---- tests ------------------------------------------------------------- */

static void test_view_classification(void)
{
    CHECK(zb_is_empty((zerobus_string_view_t){NULL, 0}));
    CHECK(zb_is_empty((zerobus_string_view_t){"x", 0}));
    CHECK(zb_is_empty((zerobus_string_view_t){NULL, 5})); /* invalid */
    CHECK(!zb_is_empty(sv("x")));

    CHECK(zb_is_valid_json(sv("{\"a\":1}")));
    CHECK(!zb_is_valid_json(sv("{bad")));
    CHECK(!zb_is_valid_json((zerobus_string_view_t){NULL, 0}));

    CHECK(zb_is_valid_string(sv("hello")));
    CHECK(!zb_is_valid_string((zerobus_string_view_t){NULL, 0}));
    /* An embedded NUL makes an otherwise non-empty view invalid. */
    CHECK(!zb_is_valid_string((zerobus_string_view_t){"a\0b", 3}));
}

static void test_utf8(void)
{
    CHECK(is_valid_utf8(NULL, 0));  /* NULL with zero length is vacuously ok */
    CHECK(!is_valid_utf8(NULL, 1)); /* NULL with a length is not */
    CHECK(utf8_ok(""));             /* empty run of bytes is valid UTF-8 */
    CHECK(utf8_ok("plain ascii"));
    CHECK(utf8_ok("caf\xC3\xA9"));         /* café (2-byte) */
    CHECK(utf8_ok("\xE2\x9C\x93"));        /* check mark (3-byte) */
    CHECK(utf8_ok("\xF0\x9F\x98\x80"));    /* emoji (4-byte) */
    CHECK(utf8_ok("caf\xC3\xA9 au lait")); /* multi-byte then more text */
    CHECK(utf8_ok("\xF4\x8F\xBF\xBF"));    /* U+10FFFF, the maximum */

    CHECK(!is_valid_utf8("a\0b", 3));             /* embedded NUL */
    CHECK(!is_valid_utf8("\x80", 1));             /* stray continuation byte */
    CHECK(!is_valid_utf8("\xFF", 1));             /* invalid lead byte */
    CHECK(!is_valid_utf8("\xC3", 1));             /* truncated 2-byte */
    CHECK(!is_valid_utf8("\xC0\xAF", 2));         /* overlong 2-byte */
    CHECK(!is_valid_utf8("\xE0\x80\x80", 3));     /* overlong 3-byte (E0) */
    CHECK(!is_valid_utf8("\xF0\x80\x80\x80", 4)); /* overlong 4-byte (F0) */
    CHECK(!is_valid_utf8("\xED\xA0\x80", 3));     /* UTF-16 surrogate U+D800 */
    CHECK(!is_valid_utf8("\xF5\x80\x80\x80", 4)); /* lead > F4 */
    CHECK(!is_valid_utf8("\xF4\x90\x80\x80", 4)); /* > U+10FFFF (F4 bound) */
    CHECK(!is_valid_utf8("\xE2\x28\xA1", 3));     /* bad first continuation */
    CHECK(!is_valid_utf8("\xE2\x9C\x28", 3));     /* bad later continuation */
}

static void test_json(void)
{
    /* Guard: is_valid_json rejects NULL and zero length up front. */
    CHECK(!is_valid_json(NULL, 0));
    CHECK(!is_valid_json("x", 0));

    /* Literals, objects, arrays, whitespace. */
    CHECK(json_ok("{}"));
    CHECK(json_ok("[]"));
    CHECK(json_ok("true"));
    CHECK(json_ok("false"));
    CHECK(json_ok("null"));
    CHECK(json_ok("{\"id\":1,\"message\":\"hello\"}"));
    CHECK(json_ok("[1, 2, 3, true, false, null]"));
    CHECK(json_ok("[[1],[2,3],{\"k\":[]}]"));
    CHECK(json_ok("  \t\n{\"k\": \"v\"}\r ")); /* surrounding whitespace ok */

    /* Numbers. */
    CHECK(json_ok("0"));
    CHECK(json_ok("-0"));
    CHECK(json_ok("123"));
    CHECK(json_ok("-12.5e+3"));
    CHECK(json_ok("1E10"));
    CHECK(json_ok("1.5e-3"));
    CHECK(!json_ok("01")); /* leading zero */
    CHECK(!json_ok("1.")); /* fraction without a digit */
    CHECK(!json_ok("-"));  /* lone minus */
    CHECK(!json_ok("1e")); /* exponent without a digit */
    CHECK(!json_ok("+1")); /* leading plus */
    CHECK(!json_ok(".5")); /* fraction without an integer part */

    /* Strings and escapes. */
    CHECK(json_ok("\"a \\\"quoted\\\" string\""));
    CHECK(json_ok("\"\\n\\t\\r\\b\\f\\/\\\\\"")); /* all single escapes */
    CHECK(json_ok("\"\\u00e9\""));                /* valid \u escape */
    CHECK(json_ok("\"\\uD800\""));  /* lone surrogate: structural only */
    CHECK(!json_ok("\"\\x\""));     /* invalid escape */
    CHECK(!json_ok("\"\\u12\""));   /* \u too short */
    CHECK(!json_ok("\"\\uzzzz\"")); /* \u bad hex */
    CHECK(!json_ok("\"\x01\""));    /* unescaped control char */
    CHECK(!json_ok("\"abc"));       /* unterminated string */
    CHECK(!json_ok("\"\\"));        /* backslash at end of input */
    CHECK(!json_ok("\xFF"));        /* non-empty but invalid UTF-8 */

    /* Object member not followed by ',' or '}'. */
    CHECK(!json_ok("{\"a\":1 2}"));

    /* Arrays. */
    CHECK(!json_ok("["));       /* unterminated */
    CHECK(!json_ok("[1"));      /* unterminated after a value */
    CHECK(!json_ok("[1, 2,]")); /* trailing comma */
    CHECK(!json_ok("[1 2]"));   /* missing comma */

    /* Objects. */
    CHECK(!json_ok("{"));          /* unterminated */
    CHECK(!json_ok("{\"k\"}"));    /* missing colon */
    CHECK(!json_ok("{\"k\": }"));  /* missing value */
    CHECK(!json_ok("{\"k\":1"));   /* unterminated after a member */
    CHECK(!json_ok("{\"k\":1,}")); /* trailing comma */
    CHECK(!json_ok("{1:2}"));      /* non-string key */

    /* Whole-value framing. */
    CHECK(!json_ok(""));       /* empty is not a JSON value */
    CHECK(!json_ok("   "));    /* whitespace only */
    CHECK(!json_ok("nul"));    /* incomplete literal */
    CHECK(!json_ok("42 abc")); /* trailing garbage after a value */
}

static void test_json_depth(void)
{
    /* A long run of bare openers must be rejected, not overflow the stack. */
    size_t runaway = ZB_JSON_MAX_DEPTH * 1000;
    char *deep = (char *)malloc(runaway);
    CHECK(deep != NULL);
    if (deep != NULL) {
        memset(deep, '[', runaway);
        CHECK(!is_valid_json(deep, runaway));
        free(deep);
    }

    /* Nesting exactly at the cap validates; one level deeper is rejected. */
    CHECK(nested_ok("[", "]", ZB_JSON_MAX_DEPTH));
    CHECK(!nested_ok("[", "]", ZB_JSON_MAX_DEPTH + 1));
    CHECK(nested_ok("{\"a\":", "}", ZB_JSON_MAX_DEPTH));
    CHECK(!nested_ok("{\"a\":", "}", ZB_JSON_MAX_DEPTH + 1));

    /* Alternating object/array nesting. */
    CHECK(nested_ok("{\"a\":[", "]}", ZB_JSON_MAX_DEPTH / 2));
    CHECK(!nested_ok("{\"a\":[", "]}", ZB_JSON_MAX_DEPTH / 2 + 1));
}

static void test_table_name(void)
{
    CHECK(zb_table_name_is_valid(sv("catalog.schema.table")));
    CHECK(zb_table_name_is_valid(sv("c.s.t")));

    CHECK(!zb_table_name_is_valid(sv("schema.table")));    /* 2 parts */
    CHECK(!zb_table_name_is_valid(sv("a.b.c.d")));         /* 4 parts */
    CHECK(!zb_table_name_is_valid(sv("catalog..table")));  /* empty middle */
    CHECK(!zb_table_name_is_valid(sv(".schema.table")));   /* leading dot */
    CHECK(!zb_table_name_is_valid(sv("catalog.schema."))); /* trailing dot */
    CHECK(!zb_table_name_is_valid(sv("catalog")));         /* no dots */
    CHECK(!zb_table_name_is_valid((zerobus_string_view_t){NULL, 0}));
}

static void test_url_validate(void)
{
    char *host = NULL;
    bool https = false;

    CHECK_EQ_INT(zb_url_validate("https://ws.zerobus.r.cloud.databricks.com",
                                 &host, &https),
                 ZB_URL_OK);
    CHECK(host != NULL &&
          strcmp(host, "ws.zerobus.r.cloud.databricks.com") == 0);
    CHECK(https);
    free(host);
    host = NULL;

    /* Port and path are stripped from the returned host. */
    CHECK_EQ_INT(
        zb_url_validate("https://host.example.com:8443/path", &host, &https),
        ZB_URL_OK);
    CHECK(host != NULL && strcmp(host, "host.example.com") == 0);
    free(host);
    host = NULL;

    /* Both out-params are optional. */
    CHECK_EQ_INT(zb_url_validate("https://host.example.com", NULL, NULL),
                 ZB_URL_OK);

    /* http is valid syntax; the scheme is reported, not rejected. Requiring
     * TLS is the caller's policy. */
    CHECK_EQ_INT(zb_url_validate("http://host.example.com", &host, &https),
                 ZB_URL_OK);
    CHECK(!https);
    free(host);
    host = NULL;

    /* A single trailing dot is the DNS root form. */
    CHECK_EQ_INT(zb_url_validate("https://host.example.com.", &host, &https),
                 ZB_URL_OK);
    free(host);
    host = NULL;

    /* No scheme at all. */
    CHECK_EQ_INT(zb_url_validate("host.example.com", &host, &https),
                 ZB_URL_NO_SCHEME);
    CHECK(host == NULL);
    CHECK_EQ_INT(zb_url_validate("ftp://host.example.com", &host, &https),
                 ZB_URL_NO_SCHEME);
    CHECK_EQ_INT(zb_url_validate(NULL, &host, &https), ZB_URL_NO_SCHEME);

    /* Missing host. */
    CHECK_EQ_INT(zb_url_validate("https://", &host, &https), ZB_URL_EMPTY_HOST);
    CHECK_EQ_INT(zb_url_validate("https:///path", &host, &https),
                 ZB_URL_EMPTY_HOST);
    CHECK_EQ_INT(zb_url_validate("https://:443", &host, &https),
                 ZB_URL_EMPTY_HOST);

    /* Empty labels are not a valid hostname. */
    CHECK_EQ_INT(zb_url_validate("https://a..b", &host, &https),
                 ZB_URL_BAD_HOST);
    CHECK(host == NULL);
    CHECK_EQ_INT(zb_url_validate("https://.a", &host, &https), ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate("https://a..b:443/p", &host, &https),
                 ZB_URL_BAD_HOST);

    /* Userinfo, IPv6 literals, and unsafe host characters are rejected. */
    CHECK_EQ_INT(
        zb_url_validate("https://user@host.example.com", &host, &https),
        ZB_URL_BAD_HOST);
    CHECK(host == NULL);
    CHECK_EQ_INT(zb_url_validate("https://[::1]", &host, &https),
                 ZB_URL_BAD_HOST);
    CHECK_EQ_INT(zb_url_validate("https://ho st.example.com", &host, &https),
                 ZB_URL_BAD_HOST);

    /* A ":port" must be a number in 1..65535. */
    CHECK_EQ_INT(zb_url_validate("https://host.example.com:", &host, &https),
                 ZB_URL_BAD_PORT);
    CHECK_EQ_INT(zb_url_validate("https://host.example.com:abc", &host, &https),
                 ZB_URL_BAD_PORT);
    CHECK_EQ_INT(
        zb_url_validate("https://host.example.com:99999", &host, &https),
        ZB_URL_BAD_PORT);
    CHECK_EQ_INT(zb_url_validate("https://host.example.com:0", &host, &https),
                 ZB_URL_BAD_PORT);
    CHECK_EQ_INT(zb_url_validate("https://host.example.com:443", &host, &https),
                 ZB_URL_OK);
    free(host);
    host = NULL;

    /* The authority ends at '?' or '#'; the host is extracted and the rest
     * ignored. */
    CHECK_EQ_INT(zb_url_validate("https://host.example.com?x=1", &host, &https),
                 ZB_URL_OK);
    CHECK(host != NULL && strcmp(host, "host.example.com") == 0);
    free(host);
    host = NULL;
    CHECK_EQ_INT(
        zb_url_validate("https://host.example.com#frag", &host, &https),
        ZB_URL_OK);
    CHECK(host != NULL && strcmp(host, "host.example.com") == 0);
    free(host);
    host = NULL;
}

static void test_host_labels(void)
{
    CHECK(labels_ok("a"));
    CHECK(labels_ok("host.example.com"));
    CHECK(labels_ok("host.example.com.")); /* trailing dot: DNS root form */

    CHECK(!labels_ok(""));     /* empty */
    CHECK(!labels_ok("."));    /* only a dot */
    CHECK(!labels_ok(".a"));   /* leading empty label */
    CHECK(!labels_ok("a..b")); /* interior empty label */
}

static void test_strdup(void)
{
    /* Each variant duplicates a non-NULL input into a fresh copy. */
    char *hello = zb_strndup("hello", 5);
    CHECK(hello != NULL && strcmp(hello, "hello") == 0);
    free(hello);

    /* Partial copy is NUL-terminated. */
    char *hel = zb_strndup("hello", 3);
    CHECK(hel != NULL && strcmp(hel, "hel") == 0);
    free(hel);

    char *from_str = zb_strdup("hello");
    CHECK(from_str != NULL && strcmp(from_str, "hello") == 0);
    free(from_str);

    char *from_view = zb_strdup_view(sv("hello"));
    CHECK(from_view != NULL && strcmp(from_view, "hello") == 0);
    free(from_view);

    /* A non-NULL pointer with length 0 yields "". */
    char *empty = zb_strndup("", 0);
    CHECK(empty != NULL && empty[0] == '\0');
    free(empty);

    char *empty_view = zb_strdup_view(sv(""));
    CHECK(empty_view != NULL && empty_view[0] == '\0');
    free(empty_view);

    /* A NULL pointer yields NULL, across every variant and length. */
    CHECK(zb_strndup(NULL, 0) == NULL);
    CHECK(zb_strndup(NULL, 5) == NULL);
    CHECK(zb_strdup_view((zerobus_string_view_t){NULL, 0}) == NULL);
    CHECK(zb_strdup_view((zerobus_string_view_t){NULL, 5}) == NULL);
    CHECK(zb_strdup(NULL) == NULL);
}

static void test_secure(void)
{
    /* Early-out paths must be safe. */
    zb_secure_zero(NULL, 8);
    zb_secure_free(NULL, 0);
    zb_secure_free_cstr(NULL); /* NULL-safe */

    char *p = zb_strndup("secret", 6);
    CHECK(p != NULL);
    if (p != NULL) {
        zb_secure_zero(p, 6); /* the actual overwrite loop */
        CHECK(p[0] == '\0');
        zb_secure_free(p, 6);
    }

    /* zb_secure_free_cstr zeroes strlen(s) bytes and frees a real copy. */
    zb_secure_free_cstr(zb_strndup("tmp", 3));
    CHECK(1);
}

int main(void)
{
    test_view_classification();
    test_utf8();
    test_json();
    test_json_depth();
    test_table_name();
    test_url_validate();
    test_host_labels();
    test_strdup();
    test_secure();
    TEST_MAIN_RETURN();
}
