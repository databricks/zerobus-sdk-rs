package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// OAuthTokenProvider obtains Unity Catalog OAuth 2.0 tokens using the client
// credentials flow and caches them until they approach expiry.
//
// UC issues downscoped tokens — each token is minted with authorization_details
// that limit it to a specific table — so the table name is part of the cache
// key. A single OAuthTokenProvider can serve many tables; tokens are cached
// independently per table.
//
// Refresh is on-access, not background: a token is re-minted on the next Token
// call once it is within the 5-minute window before expiry. If that refresh
// fails transiently the still-valid cached token is served; a non-retryable
// failure (revoked credentials, 4xx) propagates. A token left untouched past
// expiry is not refreshed until the next call, which pays a full cold mint.
//
// OAuthTokenProvider is safe for concurrent use.
type OAuthTokenProvider struct {
	clientID     string
	clientSecret string
	workspaceID  string
	ucEndpoint   string // e.g. "https://workspace.databricks.com"

	cache  *tokenCache
	client *http.Client
	logger *slog.Logger

	// cacheOpts are applied to the provider's own default cache. They are
	// discarded when a shared cache is installed via WithSharedTokenCache, since
	// that cache owns its own configuration.
	cacheOpts []CacheOption
}

// OAuthOption configures an [OAuthTokenProvider].
type OAuthOption func(*OAuthTokenProvider)

// WithHTTPClient overrides the HTTP client used for token requests. The default
// is a client with a 30-second timeout. A nil client is ignored so the default
// is preserved.
func WithHTTPClient(c *http.Client) OAuthOption {
	return func(p *OAuthTokenProvider) {
		if c != nil {
			p.client = c
		}
	}
}

// SharedTokenCache is an OAuth token cache shared across multiple
// [OAuthTokenProvider] instances via [WithSharedTokenCache]. Obtain one with
// [NewSharedTokenCache]. It exposes no methods and is safe for concurrent use.
//
// It is an opaque wrapper rather than an alias to the internal cache: a
// zero-value SharedTokenCache holds no usable cache and is ignored by
// [WithSharedTokenCache], so it cannot be mis-constructed into a nil-map panic.
type SharedTokenCache struct {
	c *tokenCache
}

// WithSharedTokenCache installs a shared token cache. A single provider already
// caches all of its tables; use this only when multiple OAuthTokenProvider
// instances should pool their tokens in one cache rather than each allocating
// its own (a single provider serves many tables on its own).
//
// Obtain a cache with [NewSharedTokenCache]. A nil or zero-value (uninitialized)
// cache is ignored so the provider's own default cache is preserved.
func WithSharedTokenCache(cache *SharedTokenCache) OAuthOption {
	return func(p *OAuthTokenProvider) {
		if cache != nil && cache.c != nil {
			p.cache = cache.c
		}
	}
}

// WithTokenCacheEnabled toggles token caching for the provider's own cache. When
// disabled, every [OAuthTokenProvider.Token] call mints a fresh token. Caching
// is enabled by default. Ignored when a shared cache is installed via
// [WithSharedTokenCache] — that cache carries its own configuration.
func WithTokenCacheEnabled(enabled bool) OAuthOption {
	return func(p *OAuthTokenProvider) {
		p.cacheOpts = append(p.cacheOpts, CacheEnabled(enabled))
	}
}

// WithRefreshBuffer sets the lead time before expiry at which the provider's own
// cache proactively re-mints a token. Defaults to 5 minutes; a non-positive
// value is ignored. Ignored when a shared cache is installed via
// [WithSharedTokenCache].
func WithRefreshBuffer(d time.Duration) OAuthOption {
	return func(p *OAuthTokenProvider) {
		p.cacheOpts = append(p.cacheOpts, CacheRefreshBuffer(d))
	}
}

// WithLogger sets the [slog.Logger] used for token-mint observability (mint
// reason, latency, retryability). A nil logger is ignored so the default
// (a no-op logger) is preserved.
func WithLogger(l *slog.Logger) OAuthOption {
	return func(p *OAuthTokenProvider) {
		if l != nil {
			p.logger = l
		}
	}
}

// NewOAuthTokenProvider creates a provider that mints UC OAuth tokens using the
// client credentials flow.
//
// zerobusEndpoint is the Zerobus service URL. The workspace ID used in OAuth
// resource audience is derived from its host prefix.
// ucEndpoint is the workspace URL (e.g. "https://my-workspace.databricks.com").
func NewOAuthTokenProvider(
	clientID, clientSecret, zerobusEndpoint, ucEndpoint string,
	opts ...OAuthOption,
) (*OAuthTokenProvider, error) {
	ucEndpoint = strings.TrimRight(strings.TrimSpace(ucEndpoint), "/")
	if ucEndpoint == "" {
		return nil, fmt.Errorf("auth: oauth: ucEndpoint is required")
	}
	if err := validateEndpoint(ucEndpoint); err != nil {
		return nil, fmt.Errorf("auth: oauth: %w", err)
	}
	if clientID == "" {
		return nil, fmt.Errorf("auth: oauth: clientID is required")
	}
	if clientSecret == "" {
		return nil, fmt.Errorf("auth: oauth: clientSecret is required")
	}
	workspaceID, err := deriveWorkspaceIDFromEndpoint(zerobusEndpoint)
	if err != nil {
		return nil, fmt.Errorf("auth: oauth: %w", err)
	}

	p := &OAuthTokenProvider{
		clientID:     clientID,
		clientSecret: clientSecret,
		workspaceID:  workspaceID,
		ucEndpoint:   ucEndpoint,
		client:       &http.Client{Timeout: 30 * time.Second},
		logger:       slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(p)
	}
	// A shared cache (set via WithSharedTokenCache) owns its own configuration;
	// otherwise build the provider's own cache from any cache options collected.
	if p.cache == nil {
		p.cache = newTokenCache(p.cacheOpts...)
	}
	p.cacheOpts = nil // closures are consumed; don't pin them on the provider.
	return p, nil
}

// NewSharedTokenCache allocates a token cache that can be passed to multiple
// [OAuthTokenProvider] instances via [WithSharedTokenCache]. Providers pool
// tokens across matching (clientID, secret, table, workspace audience, UC
// endpoint); any difference in workspace or endpoint keeps a separate entry.
//
// Configure it with [CacheEnabled] and [CacheRefreshBuffer].
func NewSharedTokenCache(opts ...CacheOption) *SharedTokenCache {
	return &SharedTokenCache{c: newTokenCache(opts...)}
}

// Token returns a valid bearer token for tableName, minting a new one via
// Unity Catalog's OIDC token endpoint when the cache is empty or nearing
// expiry.
//
// ctx bounds the token request when a mint is required. Cancelling ctx during a
// cached hit is a no-op.
//
// A successful mint is cached only when UC reports a usable expires_in. If UC
// omits expires_in (or reports a non-positive value), the token is returned but
// not cached.
func (p *OAuthTokenProvider) Token(ctx context.Context, tableName string) (string, error) {
	tableName = strings.TrimSpace(tableName)
	if err := validateTableName(tableName); err != nil {
		return "", fmt.Errorf("auth: oauth: %w", err)
	}
	return p.cache.getOrFetch(ctx, p.clientID, p.clientSecret, tableName, p.cacheScope(),
		func(ctx context.Context, reason mintReason) (fetchedToken, error) {
			return p.mint(ctx, tableName, reason)
		},
	)
}

// tokenAudience is the resource audience a minted token is bound to. It depends
// on the workspace and is sent as the OAuth request's resource field.
func (p *OAuthTokenProvider) tokenAudience() string {
	return fmt.Sprintf("api://databricks/workspaces/%s/zerobusDirectWriteApi", p.workspaceID)
}

// cacheScope discriminates cache entries that share credentials and table but
// are not interchangeable: tokens minted for different workspace audiences or
// through different UC endpoints must not be reused across a shared cache.
func (p *OAuthTokenProvider) cacheScope() string {
	return p.tokenAudience() + "\x00" + p.ucEndpoint
}

// FetchToken mints a token directly from Unity Catalog, bypassing the cache. It
// neither reads nor writes the cache and does not invalidate any existing cached
// entry, so callers get a guaranteed-fresh token; most callers should use
// [OAuthTokenProvider.Token] instead so tokens are reused across streams.
func (p *OAuthTokenProvider) FetchToken(ctx context.Context, tableName string) (string, error) {
	tableName = strings.TrimSpace(tableName)
	if err := validateTableName(tableName); err != nil {
		return "", fmt.Errorf("auth: oauth: %w", err)
	}
	fetched, err := p.mint(ctx, tableName, mintReasonDirect)
	if err != nil {
		return "", err
	}
	return fetched.token, nil
}

// mint fetches a token and emits structured observability for the outcome. It
// is the single mint entry point shared by the cached and direct paths.
func (p *OAuthTokenProvider) mint(ctx context.Context, tableName string, reason mintReason) (fetchedToken, error) {
	started := time.Now()
	fetched, err := p.fetchToken(ctx, tableName)
	elapsed := time.Since(started)

	switch {
	case err != nil:
		p.logger.LogAttrs(ctx, slog.LevelWarn, "failed to mint UC OAuth token",
			slog.String("table", tableName),
			slog.String("reason", reason.String()),
			slog.Bool("retryable", isRetryable(err)),
			slog.Duration("elapsed", elapsed),
			slog.String("error", err.Error()),
		)
	case fetched.expiresIn == nil:
		// A direct mint (FetchToken) intentionally bypasses the cache, so a
		// missing expires_in is expected there — log it at info, not as a warning
		// about an unusable cache response.
		level := slog.LevelWarn
		msg := "minted UC OAuth token but UC returned no expires_in; token will not be cached"
		if reason == mintReasonDirect {
			level = slog.LevelInfo
			msg = "minted UC OAuth token (direct, uncached)"
		}
		p.logger.LogAttrs(ctx, level, msg,
			slog.String("table", tableName),
			slog.String("reason", reason.String()),
			slog.Duration("elapsed", elapsed),
		)
	default:
		p.logger.LogAttrs(ctx, slog.LevelInfo, "minted UC OAuth token",
			slog.String("table", tableName),
			slog.String("reason", reason.String()),
			slog.Duration("expires_in", *fetched.expiresIn),
			slog.Duration("elapsed", elapsed),
		)
	}
	return fetched, err
}

// Invalidate drops the cached token for the given table so the next Token call
// re-mints from Unity Catalog. Call this when the server rejects the token with
// an authentication error.
//
// The table name is not re-validated here: it was validated at stream open, and
// invalidate is a no-op for a key with no cached entry, so an unrecognized name
// simply does nothing.
func (p *OAuthTokenProvider) Invalidate(_ context.Context, tableName string) {
	p.cache.invalidate(p.clientID, p.clientSecret, strings.TrimSpace(tableName), p.cacheScope())
}

// maxTokenResponseBytes bounds how much of an OAuth token response body is read,
// both for the success JSON and for a best-effort error payload, so a
// misbehaving server can't force an unbounded read or allocation. A legitimate
// token response is far smaller.
const maxTokenResponseBytes = 4096

// fetchToken performs the OAuth 2.0 client credentials request against Unity
// Catalog's OIDC token endpoint.
func (p *OAuthTokenProvider) fetchToken(ctx context.Context, tableName string) (fetchedToken, error) {
	catalog, schema, _, err := parseTableName(tableName)
	if err != nil {
		return fetchedToken{}, err
	}

	authDetails, err := buildAuthorizationDetails(catalog, schema, tableName)
	if err != nil {
		return fetchedToken{}, fmt.Errorf("auth: oauth: marshal authorization_details: %w", err)
	}

	form := url.Values{
		"grant_type":            {"client_credentials"},
		"scope":                 {"all-apis"},
		"resource":              {p.tokenAudience()},
		"authorization_details": {authDetails},
	}

	tokenURL := p.ucEndpoint + "/oidc/v1/token"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, tokenURL, strings.NewReader(form.Encode()))
	if err != nil {
		return fetchedToken{}, &TokenError{msg: fmt.Sprintf("build token request: %v", err), retryable: false, cause: err}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(p.clientID, p.clientSecret)

	resp, err := p.client.Do(req)
	if err != nil {
		return fetchedToken{}, &TokenError{msg: fmt.Sprintf("token request: %v", err), retryable: isRetryableTransportError(ctx, err), cause: err}
	}
	// Anchor the token's TTL to response receipt so post-receipt work (JSON
	// decode, the synchronous mint logger) can't inflate its cached lifetime.
	receivedAt := time.Now()
	// Drain unread bytes before closing so the keep-alive connection can be
	// reused: json.Decode stops at the end of the JSON value and classifyHTTPError
	// caps its read, either of which can leave a trailing tail on the body. The
	// drain is itself capped so a server appending an unbounded tail after a valid
	// response can't force an arbitrarily long read; past the cap the connection
	// is simply closed instead of reused.
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxTokenResponseBytes))
		resp.Body.Close()
	}()

	if !isHTTPSuccess(resp.StatusCode) {
		return fetchedToken{}, classifyHTTPError(resp)
	}

	var body struct {
		AccessToken string `json:"access_token"`
		// RFC 6749 §4.2.2 defines expires_in as an integer number of seconds. It
		// is captured raw and parsed tolerantly (see parseExpiresIn) so a server
		// that sends it as a string or float doesn't fail the whole mint.
		ExpiresIn json.RawMessage `json:"expires_in"`
	}
	// Bound the decode so a misbehaving server can't stream an unbounded body and
	// force arbitrary allocation. A well-formed token response is well under this.
	if err := json.NewDecoder(io.LimitReader(resp.Body, maxTokenResponseBytes)).Decode(&body); err != nil {
		return fetchedToken{}, &TokenError{
			msg:       fmt.Sprintf("parse token response: %v", err),
			retryable: false,
			cause:     err,
		}
	}
	if strings.TrimSpace(body.AccessToken) == "" {
		return fetchedToken{}, &TokenError{msg: "token response missing access_token", retryable: false}
	}
	if !isUsableAsHeader(body.AccessToken) {
		return fetchedToken{}, &TokenError{msg: "access token contains invalid header characters", retryable: false}
	}

	ft := fetchedToken{token: body.AccessToken, receivedAt: receivedAt}
	if d, ok := secondsToDuration(parseExpiresIn(body.ExpiresIn)); ok {
		ft.expiresIn = &d
	}
	return ft, nil
}

// parseExpiresIn tolerantly reads an OAuth expires_in value. RFC 6749 specifies
// an integer number of seconds, but some servers send a JSON string or float;
// all are accepted. A missing, null, or unparseable value yields 0, which
// secondsToDuration treats as "no usable TTL" so the token is still returned
// (just not cached) rather than failing the mint on a cosmetic type mismatch.
func parseExpiresIn(raw json.RawMessage) int64 {
	s := strings.TrimSpace(string(raw))
	if s == "" || s == "null" {
		return 0
	}
	// Unwrap a quoted JSON string ("3600") to its inner value before parsing.
	if unquoted, err := strconv.Unquote(s); err == nil {
		s = unquoted
	}
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n
	}
	if f, err := strconv.ParseFloat(s, 64); err == nil && f >= 0 {
		// Truncate toward zero; sub-second precision is irrelevant for a TTL.
		if f > float64(math.MaxInt64) {
			return math.MaxInt64
		}
		return int64(f)
	}
	return 0
}

// maxExpiresInSeconds bounds a server-reported expires_in so the seconds→
// nanoseconds conversion can't overflow time.Duration (an int64 nanosecond
// count, ~292 years). A larger value is clamped to this ceiling rather than
// wrapping to a negative (past) TTL.
const maxExpiresInSeconds = int64(math.MaxInt64 / int64(time.Second))

// secondsToDuration converts a positive expires_in (seconds) to a Duration,
// clamped to maxExpiresInSeconds. It returns ok=false for a non-positive value,
// which signals "no usable TTL, don't cache".
func secondsToDuration(secs int64) (time.Duration, bool) {
	if secs <= 0 {
		return 0, false
	}
	if secs > maxExpiresInSeconds {
		secs = maxExpiresInSeconds
	}
	return time.Duration(secs) * time.Second, true
}

// authorizationDetailsEntry mirrors the JSON structure sent in the OAuth request.
type authorizationDetailsEntry struct {
	Type           string   `json:"type"`
	Privileges     []string `json:"privileges"`
	ObjectType     string   `json:"object_type"`
	ObjectFullPath string   `json:"object_full_path"`
	Operations     []string `json:"operations,omitempty"`
}

func buildAuthorizationDetails(catalog, schema, fullTable string) (string, error) {
	details := []authorizationDetailsEntry{
		{
			Type:           "unity_catalog_privileges",
			Privileges:     []string{"USE CATALOG"},
			ObjectType:     "CATALOG",
			ObjectFullPath: catalog,
		},
		{
			Type:           "unity_catalog_privileges",
			Privileges:     []string{"USE SCHEMA"},
			ObjectType:     "SCHEMA",
			ObjectFullPath: catalog + "." + schema,
		},
		{
			Type:           "unity_catalog_privileges",
			Privileges:     []string{"SELECT", "MODIFY"},
			ObjectType:     "TABLE",
			ObjectFullPath: fullTable,
			Operations:     []string{"zerobuswrite"},
		},
	}
	b, err := json.Marshal(details)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

// TokenError is returned by [OAuthTokenProvider.Token] when a mint fails.
// It carries a retryability flag so the cache can decide whether to suppress
// the error and serve a still-valid cached token, and wraps the underlying
// cause (if any) so callers can inspect it with [errors.Is]/[errors.As].
//
// For an HTTP error response, Error() reports the status and the structured
// OAuth error / error_description fields (RFC 6749 §5.2) rather than the raw
// body. The raw body is retained on ResponseBody for diagnostics without
// widening the logged message.
type TokenError struct {
	msg          string
	retryable    bool
	cause        error
	ResponseBody string // raw (bounded) HTTP error body, empty for non-HTTP failures
}

func (e *TokenError) Error() string     { return "auth: oauth: " + e.msg }
func (e *TokenError) IsRetryable() bool { return e.retryable }

// Unwrap exposes the cause so [errors.Is]/[errors.As] can inspect it.
func (e *TokenError) Unwrap() error { return e.cause }

// isRetryableStatus reports whether an HTTP status is a transient failure worth
// suppressing when a cached token can be served. Only 5xx responses qualify;
// all 4xx (including 429 and 408) are non-retryable.
func isRetryableStatus(code int) bool {
	return code >= 500
}

func classifyHTTPError(resp *http.Response) error {
	// Best-effort read of the error payload, bounded so a misbehaving server
	// can't stream an unbounded body into the error message.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, maxTokenResponseBytes))
	raw := strings.TrimSpace(string(body))

	// Prefer the structured OAuth error fields (RFC 6749 §5.2) for the message so
	// the log carries a clean, parseable summary rather than the raw body; the
	// raw body is retained on the error type for diagnostics.
	msg := fmt.Sprintf("HTTP %d", resp.StatusCode)
	var oauthErr struct {
		Error       string `json:"error"`
		Description string `json:"error_description"`
	}
	if err := json.Unmarshal(body, &oauthErr); err == nil && oauthErr.Error != "" {
		msg += ": " + oauthErr.Error
		if oauthErr.Description != "" {
			msg += ": " + oauthErr.Description
		}
	}
	return &TokenError{msg: msg, retryable: isRetryableStatus(resp.StatusCode), ResponseBody: raw}
}

func isHTTPSuccess(code int) bool { return code >= 200 && code < 300 }

// isRetryableTransportError reports whether a transport-level error from the
// token request is transient and safe to retry. Network timeouts, dial
// failures, and request/budget timeouts qualify; a caller-owned cancel or
// deadline does not — that is the caller's signal to stop, not a server fault.
// isCallerCancellation tells the two apart when the error is a context error.
func isRetryableTransportError(ctx context.Context, err error) bool {
	if isContextError(err) {
		// A context error is retryable only when it came from the request itself
		// (client Timeout / transport deadline), not from the caller's own context.
		return !isCallerCancellation(ctx, err)
	}
	// Network-level timeouts (dial/read deadlines, i/o timeout) are transient.
	var ne net.Error
	if errors.As(err, &ne) && ne.Timeout() {
		return true
	}
	// A failed dial/connection (refused, reset, unreachable) is transient too.
	var oe *net.OpError
	return errors.As(err, &oe)
}

// isUsableAsHeader reports whether token can be sent as a gRPC/HTTP
// authorization header value. gRPC metadata rejects values with ASCII control
// characters (bytes 0–31 and DEL).
//
// Note: transport.isUsableAsHeaderValue applies the same character check but
// intentionally treats the empty string as usable ("no header"); this copy
// returns false for empty because a minted token must be non-empty. Keep the
// character logic in sync if either is changed.
func isUsableAsHeader(token string) bool {
	for _, r := range token {
		if r > unicode.MaxASCII || unicode.IsControl(r) {
			return false
		}
	}
	return token != ""
}

// validateEndpoint requires an https UC endpoint so client credentials never
// go over plaintext; plain http is allowed only for loopback hosts (local dev).
func validateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("ucEndpoint is not a valid URL: %w", err)
	}
	// A missing host (e.g. "https://") would otherwise pass scheme validation and
	// only fail later at request time with an opaque transport error; reject it
	// here so misconfiguration fails fast in the constructor.
	if u.Hostname() == "" {
		return fmt.Errorf("ucEndpoint has no host: %q", endpoint)
	}
	// Userinfo (user:pass@host) would be carried into the token URL and collide
	// with the SetBasicAuth client credentials, silently overriding them or
	// leaking creds in the URL. Reject it so the endpoint stays scheme://host.
	if u.User != nil {
		return fmt.Errorf("ucEndpoint must not contain userinfo: %q", endpoint)
	}
	// The token URL is formed by appending "/oidc/v1/token" to the endpoint
	// string. A query or fragment would make that suffix part of the query data
	// (or drop it entirely), silently posting client credentials to the wrong
	// path. Reject them so the endpoint is a plain scheme://host[:port][/path].
	if u.RawQuery != "" || u.ForceQuery {
		return fmt.Errorf("ucEndpoint must not contain a query string: %q", endpoint)
	}
	if u.Fragment != "" {
		return fmt.Errorf("ucEndpoint must not contain a fragment: %q", endpoint)
	}
	switch u.Scheme {
	case "https":
		return nil
	case "http":
		if isLoopbackHost(u.Hostname()) {
			return nil
		}
		return fmt.Errorf("ucEndpoint must use https (got plaintext http for non-loopback host %q); "+
			"client credentials would be sent over an unencrypted connection", u.Host)
	default:
		return fmt.Errorf("ucEndpoint must be an https URL, got scheme %q", u.Scheme)
	}
}

// isLoopbackHost reports whether host is "localhost" or a loopback IP.
// The "localhost" match is case-insensitive since DNS hostnames are.
func isLoopbackHost(host string) bool {
	if strings.EqualFold(host, "localhost") {
		return true
	}
	if ip := net.ParseIP(host); ip != nil {
		return ip.IsLoopback()
	}
	return false
}

// deriveWorkspaceIDFromEndpoint extracts workspace ID from Zerobus endpoint
// host by taking the first DNS label.
func deriveWorkspaceIDFromEndpoint(endpoint string) (workspaceID string, err error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return "", fmt.Errorf("zerobusEndpoint is required")
	}
	// URL schemes are case-insensitive (RFC 3986), so match them that way before
	// deciding whether to supply a default scheme; otherwise a valid uppercase
	// "HTTPS://host" would get a second scheme prepended and be misparsed.
	lower := strings.ToLower(endpoint)
	if !strings.HasPrefix(lower, "https://") && !strings.HasPrefix(lower, "http://") {
		endpoint = "https://" + endpoint
	}
	u, err := url.Parse(endpoint)
	if err != nil {
		return "", fmt.Errorf("zerobusEndpoint is not a valid URL: %w", err)
	}
	host := u.Hostname()
	if host == "" {
		return "", fmt.Errorf("zerobusEndpoint has no host: %q", endpoint)
	}
	// A real Zerobus endpoint always carries the workspace as the first DNS label
	// (e.g. "ws.zerobus.databricks.com"). A single-label host has no workspace
	// subdomain — almost always the workspace URL passed by mistake — and would
	// otherwise yield a syntactically valid but wrong audience whose every mint
	// 401s with an opaque error. Reject it here so the misconfig fails fast.
	if !strings.Contains(host, ".") && !isLoopbackHost(host) {
		return "", fmt.Errorf("zerobusEndpoint host %q has no workspace subdomain", host)
	}
	workspaceID, _, _ = strings.Cut(host, ".")
	if workspaceID == "" {
		return "", fmt.Errorf("failed to extract workspaceID from zerobusEndpoint host %q", host)
	}
	return workspaceID, nil
}

// validateTableName checks that s is a non-empty three-part dotted name.
func validateTableName(s string) error {
	_, _, _, err := parseTableName(s)
	return err
}

// parseTableName splits "catalog.schema.table" into its three components,
// returning an error if any part is empty or the format is wrong.
//
// Only unquoted three-part names are supported: the split is purely on ".", so
// a backtick-quoted identifier containing a dot (e.g. cat.sch.`weird.name`) is
// rejected. This matches the naming used for downscoped-token tables.
func parseTableName(s string) (catalog, schema, table string, err error) {
	// SplitN with n=4 caps the slice at 4 parts: a valid name yields exactly 3,
	// and anything with an extra dot yields 4 (rejected below) instead of
	// splitting the whole string. The table part itself must not contain a dot.
	parts := strings.SplitN(s, ".", 4)
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("table name must be catalog.schema.table, got %q", s)
	}
	catalog, schema, table = parts[0], parts[1], parts[2]
	if catalog == "" {
		return "", "", "", fmt.Errorf("catalog part of table name is empty in %q", s)
	}
	if schema == "" {
		return "", "", "", fmt.Errorf("schema part of table name is empty in %q", s)
	}
	if table == "" {
		return "", "", "", fmt.Errorf("table part of table name is empty in %q", s)
	}
	return catalog, schema, table, nil
}
