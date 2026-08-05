package zerobus

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/databricks/zerobus-sdk/purego/internal/dynamicproto"
	"github.com/databricks/zerobus-sdk/purego/internal/stream"
	"github.com/databricks/zerobus-sdk/purego/internal/zerobuspb"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/descriptorpb"
)

type retryableSchemaTestError struct{}

func (retryableSchemaTestError) Error() string     { return "retryable" }
func (retryableSchemaTestError) IsRetryable() bool { return true }

func waitForDescriptorFetchWaiters(
	t *testing.T,
	sdk *SDK,
	cacheKey string,
	want int,
) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		sdk.mu.Lock()
		got := 0
		for key, fetch := range sdk.dynamicSchemaFetches {
			if key.cacheKey == cacheKey {
				got += fetch.waiters
			}
		}
		sdk.mu.Unlock()
		if got == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("descriptor fetch waiters = %d, want %d", got, want)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestSDKFetchProtoDescriptor_CacheHit(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient:                server.Client(),
		dynamicSchemaFetchTimeout: time.Second,
	})
	b1, err := sdk.fetchProtoDescriptor(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("fetchProtoDescriptor() first call error = %v", err)
	}
	b2, err := sdk.fetchProtoDescriptor(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("fetchProtoDescriptor() second call error = %v", err)
	}
	if string(b1) != string(b2) {
		t.Fatalf("cached descriptor mismatch")
	}
	if tokenCalls.Load() != 1 {
		t.Fatalf("token calls = %d, want 1", tokenCalls.Load())
	}
	if schemaCalls.Load() != 1 {
		t.Fatalf("schema calls = %d, want 1", schemaCalls.Load())
	}
}

func TestSDKRefreshProtoDescriptorFromUC_BypassesAndReplacesCache(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			call := schemaCalls.Add(1)
			columns := []map[string]any{
				{"name": "id", "type_name": "LONG", "position": 0},
			}
			if call > 1 {
				columns = append(columns, map[string]any{
					"name": "note", "type_name": "STRING", "position": 1,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns":      columns,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	ctx := context.Background()
	first, err := sdk.FetchProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() first call error = %v", err)
	}
	cached, err := sdk.FetchProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() cached call error = %v", err)
	}
	if string(first) != string(cached) {
		t.Fatal("cached descriptor differs from first fetch")
	}

	refreshed, err := sdk.RefreshProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("RefreshProtoDescriptorFromUC() error = %v", err)
	}
	if string(first) == string(refreshed) {
		t.Fatal("refresh returned the stale descriptor")
	}
	afterRefresh, err := sdk.FetchProtoDescriptorFromUC(ctx, "main.sales.orders", "id", "secret")
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() after refresh error = %v", err)
	}
	if string(refreshed) != string(afterRefresh) {
		t.Fatal("refreshed descriptor was not cached")
	}
	if got := tokenCalls.Load(); got != 2 {
		t.Fatalf("token calls = %d, want 2", got)
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls = %d, want 2", got)
	}
}

func TestSDKRefreshProtoDescriptorFromUC_DoesNotJoinOlderFetch(t *testing.T) {
	var schemaCalls atomic.Int32
	oldFetchStarted := make(chan struct{})
	releaseOldFetch := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseOldFetch) }) }

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			call := schemaCalls.Add(1)
			columns := []map[string]any{
				{"name": "id", "type_name": "LONG", "position": 0},
			}
			if call == 1 {
				close(oldFetchStarted)
				<-releaseOldFetch
			} else {
				columns = append(columns, map[string]any{
					"name": "note", "type_name": "STRING", "position": 1,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns":      columns,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	ordinaryResult := make(chan []byte, 1)
	ordinaryErr := make(chan error, 1)
	go func() {
		desc, err := sdk.FetchProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		ordinaryResult <- desc
		ordinaryErr <- err
	}()
	select {
	case <-oldFetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary fetch did not start")
	}

	refreshCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	refreshed, err := sdk.RefreshProtoDescriptorFromUC(
		refreshCtx, "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("RefreshProtoDescriptorFromUC() error = %v", err)
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls before releasing old fetch = %d, want 2", got)
	}
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	for i := range maxDynamicSchemaCacheEntries {
		sdk.storeDynamicDescriptor(
			fmt.Sprintf("other-%03d", i),
			[]byte("other"),
		)
	}
	sdk.mu.Lock()
	_, refreshedStillCached := sdk.dynamicSchemaCache[cacheKey]
	sdk.mu.Unlock()
	if refreshedStillCached {
		t.Fatal("refreshed descriptor was not evicted for the test")
	}

	release()
	if err := <-ordinaryErr; err != nil {
		t.Fatalf("ordinary fetch error = %v", err)
	}
	ordinary := <-ordinaryResult
	if string(ordinary) == string(refreshed) {
		t.Fatal("test descriptors unexpectedly match")
	}
	sdk.mu.Lock()
	_, staleWasCached := sdk.dynamicSchemaCache[cacheKey]
	sdk.mu.Unlock()
	if staleWasCached {
		t.Fatal("older fetch repopulated the evicted cache entry")
	}
	cached, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() after refresh error = %v", err)
	}
	if string(cached) != string(refreshed) {
		t.Fatal("older fetch overwrote the refreshed descriptor")
	}
	if got := schemaCalls.Load(); got != 3 {
		t.Fatalf("schema calls = %d, want 3", got)
	}
}

func TestSDKRefreshProtoDescriptorFromUC_FailureStillSupersedesOlderFetch(t *testing.T) {
	var schemaCalls atomic.Int32
	oldFetchStarted := make(chan struct{})
	releaseOldFetch := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseOldFetch) }) }
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			switch schemaCalls.Add(1) {
			case 1:
				close(oldFetchStarted)
				<-releaseOldFetch
			case 2:
				http.Error(w, "refresh failed", http.StatusInternalServerError)
				return
			}
			columns := []map[string]any{
				{"name": "id", "type_name": "LONG", "position": 0},
			}
			if schemaCalls.Load() >= 3 {
				columns = append(columns, map[string]any{
					"name": "note", "type_name": "STRING", "position": 1,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns":      columns,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	ordinaryResult := make(chan []byte, 1)
	ordinaryErr := make(chan error, 1)
	go func() {
		desc, err := sdk.FetchProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		ordinaryResult <- desc
		ordinaryErr <- err
	}()
	select {
	case <-oldFetchStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("ordinary fetch did not start")
	}

	if _, err := sdk.RefreshProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	); err == nil {
		t.Fatal("failing refresh returned nil error")
	}
	release()
	if err := <-ordinaryErr; err != nil {
		t.Fatalf("ordinary fetch error = %v", err)
	}
	oldDescriptor := <-ordinaryResult
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	sdk.mu.Lock()
	_, oldWasCached := sdk.dynamicSchemaCache[cacheKey]
	sdk.mu.Unlock()
	if oldWasCached {
		t.Fatal("pre-refresh fetch populated the cache after refresh failed")
	}

	fresh, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("fetch after failed refresh error = %v", err)
	}
	if string(fresh) == string(oldDescriptor) {
		t.Fatal("fetch after failed refresh returned the old descriptor")
	}
	if got := schemaCalls.Load(); got != 3 {
		t.Fatalf("schema calls = %d, want 3", got)
	}
}

func TestSDKRefreshProtoDescriptorFromUC_CoalescesAndIsolatesCancellation(t *testing.T) {
	var schemaCalls atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			call := schemaCalls.Add(1)
			columns := []map[string]any{
				{"name": "id", "type_name": "LONG", "position": 0},
			}
			if call == 2 {
				close(refreshStarted)
				<-releaseRefresh
				columns = append(columns, map[string]any{
					"name": "note", "type_name": "STRING", "position": 1,
				})
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns":      columns,
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	if _, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	); err != nil {
		t.Fatalf("initial fetch error = %v", err)
	}

	leaderResult := make(chan []byte, 1)
	leaderErr := make(chan error, 1)
	go func() {
		desc, err := sdk.RefreshProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		leaderResult <- desc
		leaderErr <- err
	}()
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh request did not start")
	}

	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	sdk.mu.Lock()
	delete(sdk.dynamicSchemaCache, cacheKey)
	sdk.mu.Unlock()
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() {
		_, err := sdk.RefreshProtoDescriptorFromUC(
			waiterCtx, "main.sales.orders", "id", "secret",
		)
		waiterResult <- err
	}()
	ordinaryResult := make(chan []byte, 1)
	ordinaryErr := make(chan error, 1)
	go func() {
		desc, err := sdk.FetchProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		ordinaryResult <- desc
		ordinaryErr <- err
	}()
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 3)
	cancelWaiter()
	if err := <-waiterResult; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled refresh error = %v, want context.Canceled", err)
	}
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 2)

	release()
	if err := <-leaderErr; err != nil {
		t.Fatalf("leader refresh error = %v", err)
	}
	refreshed := <-leaderResult
	if err := <-ordinaryErr; err != nil {
		t.Fatalf("ordinary fetch error = %v", err)
	}
	if ordinary := <-ordinaryResult; string(ordinary) != string(refreshed) {
		t.Fatal("ordinary cache miss did not join the refresh")
	}
	cached, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("cached fetch error = %v", err)
	}
	if string(cached) != string(refreshed) {
		t.Fatal("refresh result was not cached")
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls = %d, want 2", got)
	}
}

func TestSDKFetchProtoDescriptor_CacheHitDoesNotJoinFailingRefresh(t *testing.T) {
	var schemaCalls atomic.Int32
	refreshStarted := make(chan struct{})
	releaseRefresh := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseRefresh) }) }
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			if schemaCalls.Add(1) == 2 {
				close(refreshStarted)
				<-releaseRefresh
				http.Error(w, "refresh failed", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	cachedDescriptor, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("initial fetch error = %v", err)
	}
	refreshResult := make(chan error, 1)
	go func() {
		_, err := sdk.RefreshProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		refreshResult <- err
	}()
	select {
	case <-refreshStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("refresh request did not start")
	}

	fetchCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	duringRefresh, err := sdk.FetchProtoDescriptorFromUC(
		fetchCtx, "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("cache hit during refresh error = %v", err)
	}
	if string(duringRefresh) != string(cachedDescriptor) {
		t.Fatal("cache hit during refresh returned different descriptor")
	}

	release()
	if err := <-refreshResult; err == nil {
		t.Fatal("failing refresh returned nil error")
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls = %d, want 2", got)
	}
}

func TestSDKFetchProtoDescriptor_CoalescesConcurrentMisses(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	schemaStarted := make(chan struct{}, 1)
	releaseSchema := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSchema) }) }

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			select {
			case schemaStarted <- struct{}{}:
			default:
			}
			<-releaseSchema
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	const callers = 16
	results := make([][]byte, callers)
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := range callers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			results[i], errs[i] = sdk.FetchProtoDescriptorFromUC(
				context.Background(), "main.sales.orders", "id", "secret",
			)
		}()
	}
	close(start)

	select {
	case <-schemaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request did not start")
	}
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, callers)
	release()
	wg.Wait()

	for i := range callers {
		if errs[i] != nil {
			t.Fatalf("caller %d error = %v", i, errs[i])
		}
		if len(results[i]) == 0 {
			t.Fatalf("caller %d returned an empty descriptor", i)
		}
		if string(results[i]) != string(results[0]) {
			t.Fatalf("caller %d returned a different descriptor", i)
		}
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("schema calls = %d, want 1", got)
	}
}

func TestSDKFetchProtoDescriptor_CallerCancellationDoesNotCancelSharedFetch(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	schemaStarted := make(chan struct{}, 1)
	releaseSchema := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseSchema) }) }

	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			select {
			case schemaStarted <- struct{}{}:
			default:
			}
			<-releaseSchema
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	leaderResult := make(chan error, 1)
	go func() {
		_, err := sdk.FetchProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		leaderResult <- err
	}()
	select {
	case <-schemaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request did not start")
	}

	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	waiterCtx, cancelWaiter := context.WithCancel(context.Background())
	waiterResult := make(chan error, 1)
	go func() {
		_, err := sdk.FetchProtoDescriptorFromUC(
			waiterCtx, "main.sales.orders", "id", "secret",
		)
		waiterResult <- err
	}()
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 2)
	cancelWaiter()
	select {
	case err := <-waiterResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancelled waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter did not return")
	}
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 1)

	release()
	select {
	case err := <-leaderResult:
		if err != nil {
			t.Fatalf("shared fetch error = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("shared fetch did not complete")
	}
	if got := tokenCalls.Load(); got != 1 {
		t.Fatalf("token calls = %d, want 1", got)
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("schema calls = %d, want 1", got)
	}
}

func TestSDKFetchProtoDescriptor_LastCancellationStopsFetch(t *testing.T) {
	schemaStarted := make(chan struct{})
	schemaCancelled := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			close(schemaStarted)
			<-r.Context().Done()
			close(schemaCancelled)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient:                server.Client(),
		dynamicSchemaFetchTimeout: time.Minute,
	})
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := sdk.FetchProtoDescriptorFromUC(
			ctx, "main.sales.orders", "id", "secret",
		)
		result <- err
	}()
	select {
	case <-schemaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request did not start")
	}

	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("fetch error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled fetch did not return")
	}
	select {
	case <-schemaCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("shared schema request was not cancelled")
	}
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, 0)
}

func TestSDKCloseCancelsDescriptorFetch(t *testing.T) {
	schemaStarted := make(chan struct{})
	schemaCancelled := make(chan struct{})
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			close(schemaStarted)
			<-r.Context().Done()
			close(schemaCancelled)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk, err := New(
		"https://workspace.zerobus.cloud.databricks.com",
		server.URL,
		WithHTTPClient(server.Client()),
		WithProtoDescriptorFetchTimeout(time.Minute),
	)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	result := make(chan error, 1)
	go func() {
		_, err := sdk.FetchProtoDescriptorFromUC(
			context.Background(), "main.sales.orders", "id", "secret",
		)
		result <- err
	}()
	select {
	case <-schemaStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request did not start")
	}

	if err := sdk.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
	select {
	case err := <-result:
		if !errors.Is(err, errSDKClosed) {
			t.Fatalf("fetch error = %v, want SDK closed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("fetch waiter was not released by Close")
	}
	select {
	case <-schemaCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("schema request was not cancelled by Close")
	}
}

func TestSDKFetchProtoDescriptor_FailedSharedFetchCanRetry(t *testing.T) {
	var schemaCalls atomic.Int32
	firstStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(releaseFirst) }) }
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			if schemaCalls.Add(1) == 1 {
				close(firstStarted)
				<-releaseFirst
				http.Error(w, "temporary failure", http.StatusInternalServerError)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	defer release()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	const callers = 2
	start := make(chan struct{})
	errs := make(chan error, callers)
	for range callers {
		go func() {
			<-start
			_, err := sdk.FetchProtoDescriptorFromUC(
				context.Background(), "main.sales.orders", "id", "secret",
			)
			errs <- err
		}()
	}
	close(start)
	select {
	case <-firstStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("shared failing request did not start")
	}
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	waitForDescriptorFetchWaiters(t, sdk, cacheKey, callers)
	release()
	for range callers {
		if err := <-errs; err == nil {
			t.Fatal("shared failing fetch returned nil error")
		}
	}

	if _, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	); err != nil {
		t.Fatalf("retry after shared failure error = %v", err)
	}
	if got := schemaCalls.Load(); got != 2 {
		t.Fatalf("schema calls = %d, want 2", got)
	}
}

func TestStreamEncodeJSONBatch(t *testing.T) {
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Order"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_REQUIRED.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
			},
		},
	}
	b, err := proto.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	c, err := dynamicproto.NewFromDescriptorProtoBytes(b)
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}
	ds := &Stream{
		recordType:     zerobuspb.RecordType_PROTO,
		jsonConverter:  c,
		conversionGate: make(chan struct{}, 1),
	}
	out, err := ds.encodeJSONBatchContext(context.Background(), [][]byte{
		[]byte(`{"id":1}`),
		[]byte(`{"id":2}`),
	})
	if err != nil {
		t.Fatalf("encodeJSONBatchContext() error = %v", err)
	}
	if len(out) != 2 {
		t.Fatalf("batch len = %d, want 2", len(out))
	}
}

func TestStreamMessageDescriptor(t *testing.T) {
	desc := &descriptorpb.DescriptorProto{
		Name: proto.String("Order"),
		Field: []*descriptorpb.FieldDescriptorProto{
			{
				Name:   proto.String("id"),
				Number: proto.Int32(1),
				Label:  descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:   descriptorpb.FieldDescriptorProto_TYPE_INT64.Enum(),
			},
		},
	}
	b, err := proto.Marshal(desc)
	if err != nil {
		t.Fatalf("marshal descriptor: %v", err)
	}
	converter, err := dynamicproto.NewFromDescriptorProtoBytes(b)
	if err != nil {
		t.Fatalf("NewFromDescriptorProtoBytes() error = %v", err)
	}

	stream := &Stream{jsonConverter: converter}
	got := stream.MessageDescriptor()
	if got == nil {
		t.Fatal("MessageDescriptor() = nil")
	}
	if got.Name() != "Order" {
		t.Fatalf("MessageDescriptor().Name() = %q, want Order", got.Name())
	}
	if got.Fields().ByName("id") == nil {
		t.Fatal("MessageDescriptor() missing id field")
	}
}

func TestStreamJSONConversionRejectsUnsupportedDescriptor(t *testing.T) {
	message := &descriptorpb.DescriptorProto{Name: proto.String("Order")}
	fileBytes, err := proto.Marshal(&descriptorpb.FileDescriptorProto{
		Name:        proto.String("orders.proto"),
		Syntax:      proto.String("proto2"),
		MessageType: []*descriptorpb.DescriptorProto{message},
	})
	if err != nil {
		t.Fatalf("marshal FileDescriptorProto: %v", err)
	}
	for name, descriptor := range map[string][]byte{
		"malformed":           []byte("bad descriptor"),
		"FileDescriptorProto": fileBytes,
	} {
		t.Run(name, func(t *testing.T) {
			_, converterErr := dynamicproto.NewFromDescriptorProtoBytes(descriptor)
			if converterErr == nil {
				t.Fatal("descriptor unexpectedly supports JSON conversion")
			}
			stream := &Stream{
				recordType:       zerobuspb.RecordType_PROTO,
				jsonConverterErr: converterErr,
				conversionGate:   make(chan struct{}, 1),
			}
			if _, err := stream.IngestJSONOffset([]byte(`{"id":1}`)); err == nil ||
				!strings.Contains(err.Error(), "JSON conversion is unavailable") {
				t.Fatalf("IngestJSONOffset() error = %v, want conversion error", err)
			}
		})
	}
}

func TestSDKFetchProtoDescriptor_CacheIsCredentialScoped(t *testing.T) {
	var tokenCalls atomic.Int32
	var schemaCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			tokenCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	for _, credentials := range [][2]string{
		{"id-1", "secret-1"},
		{"id-2", "secret-1"},
		{"id-1", "secret-2"},
	} {
		if _, err := sdk.fetchProtoDescriptor(
			context.Background(), "main.sales.orders", credentials[0], credentials[1],
		); err != nil {
			t.Fatalf("fetchProtoDescriptor() error = %v", err)
		}
	}
	if got := tokenCalls.Load(); got != 3 {
		t.Fatalf("token calls = %d, want 3", got)
	}
	if got := schemaCalls.Load(); got != 3 {
		t.Fatalf("schema calls = %d, want 3", got)
	}
}

func TestSDKFetchProtoDescriptor_ExpiredEntryRefetches(t *testing.T) {
	var schemaCalls atomic.Int32
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/oidc/v1/token":
			_ = json.NewEncoder(w).Encode(map[string]any{"access_token": "abc"})
		case strings.HasPrefix(r.URL.Path, "/api/2.1/unity-catalog/tables/"):
			schemaCalls.Add(1)
			_ = json.NewEncoder(w).Encode(map[string]any{
				"name":         "orders",
				"catalog_name": "main",
				"schema_name":  "sales",
				"columns": []map[string]any{
					{"name": "id", "type_name": "LONG", "position": 0},
				},
			})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	sdk := newSDK(nil, "https://workspace.zerobus.cloud.databricks.com", server.URL, sdkConfig{
		httpClient: server.Client(),
	})
	cacheKey := dynamicSchemaCacheKey(server.URL, "main.sales.orders", "id", "secret")
	sdk.dynamicSchemaCache[cacheKey] = cachedDescriptor{
		descriptor: []byte("stale"),
		expiresAt:  time.Now().Add(-time.Second),
	}
	desc, err := sdk.FetchProtoDescriptorFromUC(
		context.Background(), "main.sales.orders", "id", "secret",
	)
	if err != nil {
		t.Fatalf("FetchProtoDescriptorFromUC() error = %v", err)
	}
	if string(desc) == "stale" {
		t.Fatal("expired descriptor was returned")
	}
	if got := schemaCalls.Load(); got != 1 {
		t.Fatalf("schema calls = %d, want 1", got)
	}
}

func TestSDKStoreDynamicDescriptor_PrunesExpiredEntries(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	sdk.dynamicSchemaCache["old"] = cachedDescriptor{
		descriptor: []byte("old"),
		expiresAt:  time.Now().Add(-time.Second),
	}
	sdk.storeDynamicDescriptor("new", []byte("new"))
	if len(sdk.dynamicSchemaCache) != 1 {
		t.Fatalf("cache entries = %d, want 1", len(sdk.dynamicSchemaCache))
	}
	if _, ok := sdk.dynamicSchemaCache["new"]; !ok {
		t.Fatal("new cache entry missing")
	}
}

func TestSDKStoreDynamicDescriptor_EnforcesEntryLimit(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	for i := range maxDynamicSchemaCacheEntries + 1 {
		sdk.storeDynamicDescriptor(fmt.Sprintf("entry-%03d", i), []byte("descriptor"))
	}
	if got := len(sdk.dynamicSchemaCache); got != maxDynamicSchemaCacheEntries {
		t.Fatalf("cache entries = %d, want %d", got, maxDynamicSchemaCacheEntries)
	}
	if _, ok := sdk.dynamicSchemaCache["entry-000"]; ok {
		t.Fatal("oldest descriptor was not evicted")
	}
	if _, ok := sdk.dynamicSchemaCache[fmt.Sprintf("entry-%03d", maxDynamicSchemaCacheEntries)]; !ok {
		t.Fatal("newest descriptor was not cached")
	}
}

func TestSDKStoreDynamicDescriptor_DoesNotPopulateClosedSDK(t *testing.T) {
	sdk := newSDK(nil, "https://zerobus", "https://uc", sdkConfig{})
	sdk.mu.Lock()
	sdk.closed = true
	sdk.mu.Unlock()
	sdk.storeDynamicDescriptor("closed", []byte("descriptor"))
	if len(sdk.dynamicSchemaCache) != 0 {
		t.Fatal("closed SDK cache was populated")
	}
}

func TestStreamRejectsJSONBatchBeforeConversion(t *testing.T) {
	ds := &Stream{
		recordType:              zerobuspb.RecordType_PROTO,
		conversionGate:          make(chan struct{}, 1),
		maxBatchRecords:         1,
		maxBufferedPayloadBytes: 64,
	}
	if _, err := ds.IngestJSONRecordsOffset([][]byte{[]byte(`{}`), []byte(`{}`)}); !errors.Is(err, stream.ErrPayloadTooLarge) {
		t.Fatalf("batch count error = %v, want ErrPayloadTooLarge", err)
	}
	if _, err := ds.IngestJSONOffset(make([]byte, 65)); !errors.Is(err, stream.ErrPayloadTooLarge) {
		t.Fatalf("buffered payload error = %v, want ErrPayloadTooLarge", err)
	}
}

func TestDynamicSchemaErrorRetryable(t *testing.T) {
	if dynamicSchemaErrorRetryable(errors.New("bad schema")) {
		t.Fatal("plain schema validation error classified retryable")
	}
	if !dynamicSchemaErrorRetryable(retryableSchemaTestError{}) {
		t.Fatal("self-classified fetch error classified non-retryable")
	}
}
