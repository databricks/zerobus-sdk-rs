// Package ucschema fetches Unity Catalog table schemas through REST.
package ucschema

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
	"unicode"

	"github.com/databricks/zerobus-sdk/purego/internal/schema"
)

const (
	defaultHTTPTimeout     = 30 * time.Second
	defaultRequestTimeout  = 10 * time.Second
	maxTokenResponseBytes  = 16 * 1024
	maxSchemaResponseBytes = 2 * 1024 * 1024
)

var errRequestTimeout = errors.New("uc schema request timeout")

// FetchError reports token/schema fetch failures with retryability.
type FetchError struct {
	op        string
	msg       string
	cause     error
	retryable bool
}

func (e *FetchError) Error() string {
	prefix := "uc schema fetch"
	if e.op != "" {
		prefix += " " + e.op
	}
	if e.msg == "" {
		return prefix + " failed"
	}
	return prefix + ": " + e.msg
}

func (e *FetchError) Unwrap() error     { return e.cause }
func (e *FetchError) IsRetryable() bool { return e.retryable }

// Config configures a schema fetcher.
type Config struct {
	WorkspaceEndpoint string
	ClientID          string
	ClientSecret      string
	HTTPClient        *http.Client
	RequestTimeout    time.Duration
}

// Fetcher fetches UC table schemas using OAuth client credentials.
type Fetcher struct {
	workspaceEndpoint string
	clientID          string
	clientSecret      string
	client            *http.Client
	requestTimeout    time.Duration
}

// New creates a UC schema fetcher.
func New(cfg Config) (*Fetcher, error) {
	endpoint := strings.TrimRight(strings.TrimSpace(cfg.WorkspaceEndpoint), "/")
	if endpoint == "" {
		return nil, fmt.Errorf("ucschema: workspace endpoint is required")
	}
	if err := validateEndpoint(endpoint); err != nil {
		return nil, fmt.Errorf("ucschema: %w", err)
	}
	if strings.TrimSpace(cfg.ClientID) == "" {
		return nil, fmt.Errorf("ucschema: clientID is required")
	}
	if strings.TrimSpace(cfg.ClientSecret) == "" {
		return nil, fmt.Errorf("ucschema: clientSecret is required")
	}
	client := cfg.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: defaultHTTPTimeout}
	}
	timeout := cfg.RequestTimeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}
	return &Fetcher{
		workspaceEndpoint: endpoint,
		clientID:          cfg.ClientID,
		clientSecret:      cfg.ClientSecret,
		client:            client,
		requestTimeout:    timeout,
	}, nil
}

// FetchTableSchema fetches UC schema metadata for tableFullName.
func (f *Fetcher) FetchTableSchema(ctx context.Context, tableFullName string) (*schema.UcTableSchema, error) {
	tableFullName = strings.TrimSpace(tableFullName)
	if err := validateTableName(tableFullName); err != nil {
		return nil, &FetchError{op: "FetchTableSchema", msg: err.Error(), retryable: false, cause: err}
	}
	callCtx, cancel := context.WithTimeoutCause(ctx, f.requestTimeout, errRequestTimeout)
	defer cancel()

	token, err := f.fetchToken(callCtx)
	if err != nil {
		return nil, err
	}

	endpoint := f.workspaceEndpoint + "/api/2.1/unity-catalog/tables/" + url.PathEscape(tableFullName)
	req, err := http.NewRequestWithContext(callCtx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, &FetchError{op: "FetchTableSchema", msg: fmt.Sprintf("build schema request: %v", err), retryable: false, cause: err}
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")

	resp, err := f.doNoRedirect(req)
	if err != nil {
		return nil, &FetchError{
			op:        "FetchTableSchema",
			msg:       fmt.Sprintf("schema request: %v", err),
			retryable: isRetryableTransportError(callCtx, err),
			cause:     err,
		}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxSchemaResponseBytes))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode/100 != 2 {
		return nil, classifyHTTPError("FetchTableSchema", resp)
	}

	var body struct {
		Name        string            `json:"name"`
		CatalogName string            `json:"catalog_name"`
		SchemaName  string            `json:"schema_name"`
		Columns     []schema.UcColumn `json:"columns"`
	}
	if err := decodeBoundedJSON(resp.Body, maxSchemaResponseBytes, &body); err != nil {
		return nil, &FetchError{
			op:        "FetchTableSchema",
			msg:       fmt.Sprintf("parse schema response: %v", err),
			retryable: isRetryableTransportError(callCtx, err),
			cause:     err,
		}
	}
	if len(body.Columns) == 0 {
		return nil, &FetchError{
			op:        "FetchTableSchema",
			msg:       "schema response contains no columns",
			retryable: false,
		}
	}
	if strings.TrimSpace(body.Name) == "" {
		body.Name = tableNameFromFullName(tableFullName)
	}
	if strings.TrimSpace(body.SchemaName) == "" || strings.TrimSpace(body.CatalogName) == "" {
		catalog, sch, _, splitErr := splitTableName(tableFullName)
		if splitErr != nil {
			return nil, &FetchError{op: "FetchTableSchema", msg: splitErr.Error(), retryable: false, cause: splitErr}
		}
		if strings.TrimSpace(body.CatalogName) == "" {
			body.CatalogName = catalog
		}
		if strings.TrimSpace(body.SchemaName) == "" {
			body.SchemaName = sch
		}
	}
	return &schema.UcTableSchema{
		Name:        body.Name,
		CatalogName: body.CatalogName,
		SchemaName:  body.SchemaName,
		Columns:     body.Columns,
	}, nil
}

func (f *Fetcher) fetchToken(ctx context.Context) (string, error) {
	form := url.Values{
		"grant_type": {"client_credentials"},
		"scope":      {"all-apis"},
	}
	endpoint := f.workspaceEndpoint + "/oidc/v1/token"
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint, strings.NewReader(form.Encode()))
	if err != nil {
		return "", &FetchError{op: "FetchToken", msg: fmt.Sprintf("build token request: %v", err), retryable: false, cause: err}
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.SetBasicAuth(f.clientID, f.clientSecret)

	resp, err := f.doNoRedirect(req)
	if err != nil {
		return "", &FetchError{
			op:        "FetchToken",
			msg:       fmt.Sprintf("token request: %v", err),
			retryable: isRetryableTransportError(ctx, err),
			cause:     err,
		}
	}
	defer func() {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxTokenResponseBytes))
		_ = resp.Body.Close()
	}()
	if resp.StatusCode/100 != 2 {
		return "", classifyHTTPError("FetchToken", resp)
	}

	var body struct {
		AccessToken string `json:"access_token"`
	}
	if err := decodeBoundedJSON(resp.Body, maxTokenResponseBytes, &body); err != nil {
		return "", &FetchError{
			op:        "FetchToken",
			msg:       fmt.Sprintf("parse token response: %v", err),
			retryable: isRetryableTransportError(ctx, err),
			cause:     err,
		}
	}
	if strings.TrimSpace(body.AccessToken) == "" {
		return "", &FetchError{op: "FetchToken", msg: "token response missing access_token", retryable: false}
	}
	if !isUsableAsHeader(body.AccessToken) {
		return "", &FetchError{
			op:        "FetchToken",
			msg:       "token response contains invalid access_token",
			retryable: false,
		}
	}
	return body.AccessToken, nil
}

func (f *Fetcher) doNoRedirect(req *http.Request) (*http.Response, error) {
	client := *f.client
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client.Do(req)
}

func classifyHTTPError(op string, resp *http.Response) error {
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	trimmed := strings.TrimSpace(string(body))
	msg := fmt.Sprintf("%s HTTP %d", op, resp.StatusCode)
	if trimmed != "" {
		msg += ": " + trimmed
	}
	return &FetchError{
		op:        op,
		msg:       msg,
		retryable: resp.StatusCode >= 500 || resp.StatusCode == http.StatusTooManyRequests,
	}
}

func decodeBoundedJSON(r io.Reader, limit int64, destination any) error {
	data, err := io.ReadAll(io.LimitReader(r, limit+1))
	if err != nil {
		return err
	}
	if int64(len(data)) > limit {
		return fmt.Errorf("response exceeds %d bytes", limit)
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	if err := decoder.Decode(destination); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("response contains multiple JSON values")
		}
		return fmt.Errorf("response contains trailing data: %w", err)
	}
	return nil
}

func isRetryableTransportError(ctx context.Context, err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return errors.Is(context.Cause(ctx), errRequestTimeout)
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return true
	}
	var uerr *url.Error
	return errors.As(err, &uerr)
}

func validateEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil {
		return fmt.Errorf("uc endpoint is not a valid URL: %w", err)
	}
	if strings.ToLower(u.Scheme) != "https" {
		return fmt.Errorf("uc endpoint must use HTTPS")
	}
	if u.Hostname() == "" {
		return fmt.Errorf("uc endpoint host is empty")
	}
	if u.User != nil {
		return fmt.Errorf("uc endpoint must not contain userinfo")
	}
	if u.RawQuery != "" || u.ForceQuery {
		return fmt.Errorf("uc endpoint must not contain a query string")
	}
	if u.Fragment != "" {
		return fmt.Errorf("uc endpoint must not contain a fragment")
	}
	return nil
}

func isUsableAsHeader(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if r > unicode.MaxASCII || unicode.IsControl(r) {
			return false
		}
	}
	return true
}

func validateTableName(tableName string) error {
	_, _, _, err := splitTableName(tableName)
	return err
}

func splitTableName(tableName string) (catalog, schemaName, table string, err error) {
	parts := strings.Split(strings.TrimSpace(tableName), ".")
	if len(parts) != 3 {
		return "", "", "", fmt.Errorf("table name must have catalog.schema.table format")
	}
	for _, p := range parts {
		if strings.TrimSpace(p) == "" {
			return "", "", "", fmt.Errorf("table name must have catalog.schema.table format")
		}
	}
	return parts[0], parts[1], parts[2], nil
}

func tableNameFromFullName(fullName string) string {
	_, _, table, err := splitTableName(fullName)
	if err != nil {
		return fullName
	}
	return table
}
