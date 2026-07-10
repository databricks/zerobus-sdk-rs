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
	tableName    string

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
type SharedTokenCache = tokenCache

// WithSharedTokenCache installs a shared token cache. Use this when multiple
// OAuthTokenProviders are created for the same credentials but different tables
// so they share one cache map rather than each allocating their own.
//
// Obtain a cache with [NewSharedTokenCache]. A nil cache is ignored so the
// provider's own default cache is preserved.
func WithSharedTokenCache(cache *SharedTokenCache) OAuthOption {
	return func(p *OAuthTokenProvider) {
		if cache != nil {
			p.cache = cache
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

// NewOAuthTokenProvider creates a provider that mints UC OAuth tokens for
// tableName using the client credentials flow.
//
// ucEndpoint is the workspace URL (e.g. "https://my-workspace.databricks.com").
// workspaceID is the numeric Databricks workspace ID.
func NewOAuthTokenProvider(
	clientID, clientSecret, workspaceID, ucEndpoint, tableName string,
	opts ...OAuthOption,
) (*OAuthTokenProvider, error) {
	tableName = strings.TrimSpace(tableName)
	if err := validateTableName(tableName); err != nil {
		return nil, fmt.Errorf("auth: oauth: %w", err)
	}
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
	if workspaceID == "" {
		return nil, fmt.Errorf("auth: oauth: workspaceID is required")
	}

	p := &OAuthTokenProvider{
		clientID:     clientID,
		clientSecret: clientSecret,
		workspaceID:  workspaceID,
		ucEndpoint:   ucEndpoint,
		tableName:    tableName,
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
	return p, nil
}

// NewSharedTokenCache allocates a token cache that can be passed to multiple
// [OAuthTokenProvider] instances via [WithSharedTokenCache]. Sharing a cache
// ensures tokens for the same (clientID, secret, table) are reused across
// providers rather than each minting independently.
//
// Configure it with [CacheEnabled] and [CacheRefreshBuffer].
func NewSharedTokenCache(opts ...CacheOption) *SharedTokenCache {
	return newTokenCache(opts...)
}

// Token returns a valid bearer token for the provider's table, minting a new
// one via Unity Catalog's OIDC token endpoint when the cache is empty or
// nearing expiry.
//
// ctx bounds the token request when a mint is required. Cancelling ctx during a
// cached hit is a no-op.
//
// Every successful mint is cached. If UC reports an expires_in it is honored; if
// it omits one the token is cached under a bounded default lifetime instead of
// being re-minted on every call. The proactive-refresh lead time is clamped to
// at most half the token's TTL, so even a short-lived token gets some reuse
// before it is refreshed.
func (p *OAuthTokenProvider) Token(ctx context.Context) (string, error) {
	return p.cache.getOrFetch(ctx, p.clientID, p.clientSecret, p.tableName, p.mint)
}

// FetchToken mints a token directly from Unity Catalog, bypassing the cache. It
// neither reads nor writes the cache, so callers get a guaranteed-fresh token;
// most callers should use [OAuthTokenProvider.Token] instead so tokens are
// reused across streams.
func (p *OAuthTokenProvider) FetchToken(ctx context.Context) (string, error) {
	fetched, err := p.mint(ctx, mintReasonDirect)
	if err != nil {
		return "", err
	}
	return fetched.token, nil
}

// mint fetches a token and emits structured observability for the outcome. It
// is the single mint entry point shared by the cached and direct paths.
func (p *OAuthTokenProvider) mint(ctx context.Context, reason mintReason) (fetchedToken, error) {
	started := time.Now()
	fetched, err := p.fetchToken(ctx)
	elapsed := time.Since(started)

	switch {
	case err != nil:
		p.logger.LogAttrs(ctx, slog.LevelWarn, "failed to mint UC OAuth token",
			slog.String("table", p.tableName),
			slog.String("reason", reason.String()),
			slog.Bool("retryable", isRetryable(err)),
			slog.Duration("elapsed", elapsed),
			slog.String("error", err.Error()),
		)
	case fetched.expiresIn == nil:
		p.logger.LogAttrs(ctx, slog.LevelWarn, "minted UC OAuth token but UC returned no expires_in; caching under a conservative default lifetime",
			slog.String("table", p.tableName),
			slog.String("reason", reason.String()),
			slog.Duration("elapsed", elapsed),
		)
	default:
		p.logger.LogAttrs(ctx, slog.LevelInfo, "minted UC OAuth token",
			slog.String("table", p.tableName),
			slog.String("reason", reason.String()),
			slog.Duration("expires_in", *fetched.expiresIn),
			slog.Duration("elapsed", elapsed),
		)
	}
	return fetched, err
}

// Invalidate drops the cached token for this provider's table so the next
// Token call re-mints from Unity Catalog. Call this when the server rejects
// the token with an authentication error.
func (p *OAuthTokenProvider) Invalidate(_ context.Context) {
	p.cache.invalidate(p.clientID, p.clientSecret, p.tableName)
}

// fetchToken performs the OAuth 2.0 client credentials request against Unity
// Catalog's OIDC token endpoint.
func (p *OAuthTokenProvider) fetchToken(ctx context.Context) (fetchedToken, error) {
	catalog, schema, _, err := parseTableName(p.tableName)
	if err != nil {
		return fetchedToken{}, err
	}

	authDetails, err := buildAuthorizationDetails(catalog, schema, p.tableName)
	if err != nil {
		return fetchedToken{}, fmt.Errorf("auth: oauth: marshal authorization_details: %w", err)
	}

	form := url.Values{
		"grant_type":            {"client_credentials"},
		"scope":                 {"all-apis"},
		"resource":              {fmt.Sprintf("api://databricks/workspaces/%s/zerobusDirectWriteApi", p.workspaceID)},
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
		return fetchedToken{}, &TokenError{msg: fmt.Sprintf("token request: %v", err), retryable: isRetryableTransportError(err), cause: err}
	}
	// Drain any unread bytes before closing so the keep-alive connection can be
	// reused: json.Decode stops at the end of the JSON value and classifyHTTPError
	// caps its read, either of which can leave a trailing tail on the body.
	defer func() {
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}()

	if !isHTTPSuccess(resp.StatusCode) {
		return fetchedToken{}, classifyHTTPError(resp)
	}

	var body struct {
		AccessToken string `json:"access_token"`
		// RFC 6749 §4.2.2 defines expires_in as an integer number of seconds.
		ExpiresIn int64 `json:"expires_in"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return fetchedToken{}, &TokenError{
			msg:       fmt.Sprintf("parse token response: %v", err),
			retryable: false,
			cause:     err,
		}
	}
	if body.AccessToken == "" {
		return fetchedToken{}, &TokenError{msg: "token response missing access_token", retryable: false}
	}
	if !isUsableAsHeader(body.AccessToken) {
		return fetchedToken{}, &TokenError{msg: "access token contains invalid header characters", retryable: false}
	}

	ft := fetchedToken{token: body.AccessToken}
	if d, ok := secondsToDuration(body.ExpiresIn); ok {
		ft.expiresIn = &d
	}
	return ft, nil
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
type TokenError struct {
	msg       string
	retryable bool
	cause     error
}

func (e *TokenError) Error() string     { return "auth: oauth: " + e.msg }
func (e *TokenError) IsRetryable() bool { return e.retryable }

// Unwrap exposes the cause so [errors.Is]/[errors.As] can inspect it.
func (e *TokenError) Unwrap() error { return e.cause }

// isRetryableStatus reports whether an HTTP status is a transient failure worth
// suppressing when a cached token can be served. Only 5xx responses qualify;
// all 4xx (including 429 and 408) are non-retryable, matching the Rust SDK.
func isRetryableStatus(code int) bool {
	return code >= 500
}

func classifyHTTPError(resp *http.Response) error {
	// Best-effort read of the error payload, bounded so a misbehaving server
	// can't stream an unbounded body into the error message.
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	detail := strings.TrimSpace(string(body))
	msg := fmt.Sprintf("HTTP %d", resp.StatusCode)
	if detail != "" {
		msg += ": " + detail
	}
	return &TokenError{msg: msg, retryable: isRetryableStatus(resp.StatusCode)}
}

func isHTTPSuccess(code int) bool { return code >= 200 && code < 300 }

// isRetryableTransportError reports whether a transport-level error from the
// token request is transient and safe to retry. Only timeouts and connection
// failures qualify; a cancelled context is the caller's own signal, and any
// other transport error is treated as non-retryable so a genuine failure isn't
// masked by a stale cached token.
func isRetryableTransportError(err error) bool {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return false
	}
	// Timeouts (client Timeout, dial/read deadlines) are transient.
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
// Note: transport.isUsableAsHeader applies the same character check but
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
