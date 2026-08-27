"""Tests for the external-IdP federation auth surface (FederatedToken).

These cover the pure-Python dispatch layer of ``create_stream`` with a fake
native ``_inner`` so no network or gRPC server is needed. The Rust core's
exchange/caching behavior is covered by the Rust unit tests; the Python->Rust
callback bridge is exercised end-to-end separately against a mock token
endpoint.
"""

import pytest

import zerobus
from zerobus import FederatedToken, TableProperties, ZerobusSdk
from zerobus.sdk.aio import ZerobusSdk as AsyncZerobusSdk


class _FakeInner:
    """Records which native create_stream_* method the wrapper dispatched to."""

    def __init__(self):
        self.calls = []

    def create_stream_federated(self, table_properties, idp_token_supplier, databricks_client_id, cache_key, options):
        self.calls.append(("federated", table_properties, idp_token_supplier, databricks_client_id, cache_key, options))
        return object()

    def create_stream(self, client_id, client_secret, table_properties, options):
        self.calls.append(("oauth", client_id, client_secret, table_properties, options))
        return object()

    def create_stream_with_headers_provider(self, table_properties, headers_provider, options):
        self.calls.append(("headers", table_properties, headers_provider, options))
        return object()


class _AsyncFakeInner:
    def __init__(self):
        self.calls = []

    async def create_stream_federated(
        self, table_properties, idp_token_supplier, databricks_client_id, cache_key, options
    ):
        self.calls.append(("federated", table_properties, idp_token_supplier, databricks_client_id, cache_key, options))
        return object()

    async def create_stream(self, client_id, client_secret, table_properties, options):
        self.calls.append(("oauth", client_id, client_secret, table_properties, options))
        return object()

    async def create_stream_with_headers_provider(self, table_properties, headers_provider, options):
        self.calls.append(("headers", table_properties, headers_provider, options))
        return object()


def _sync_sdk_with_fake():
    sdk = ZerobusSdk(host="https://example", unity_catalog_url="https://example")
    fake = _FakeInner()
    sdk._inner = fake
    return sdk, fake


def _props():
    return TableProperties("cat.sch.tbl")


def test_federated_token_exported_and_constructs():
    assert "FederatedToken" in zerobus.__all__
    account = FederatedToken(idp_token_supplier=lambda: "tok")
    assert account.databricks_client_id is None
    workload = FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp-uuid")
    assert workload.databricks_client_id == "sp-uuid"


def test_native_methods_present():
    import zerobus._zerobus_core as _core

    assert hasattr(_core.sync.ZerobusSdk, "create_stream_federated")
    assert hasattr(_core.aio.ZerobusSdk, "create_stream_federated")


def test_create_stream_routes_auth_to_federated_account_level():
    sdk, fake = _sync_sdk_with_fake()

    def supplier():
        return "tok"

    sdk.create_stream(table_properties=_props(), auth=FederatedToken(idp_token_supplier=supplier))

    assert len(fake.calls) == 1
    kind, _tp, passed_supplier, client_id, _ck, _opts = fake.calls[0]
    assert kind == "federated"
    assert passed_supplier is supplier
    assert client_id is None


def test_create_stream_routes_auth_to_federated_workload():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp-uuid"),
    )
    kind, _tp, _sup, client_id, _ck, _opts = fake.calls[0]
    assert kind == "federated"
    assert client_id == "sp-uuid"


def test_distinct_federated_tokens_get_distinct_cache_keys():
    # Two FederatedToken instances (different account-level identities) must pass
    # distinct cache_keys, so the shared token cache isolates them instead of the
    # second stream reusing the first's exchanged token. Reusing one instance
    # passes the same key, so its cache stays shared.
    sdk, fake = _sync_sdk_with_fake()
    a = FederatedToken(idp_token_supplier=lambda: "tok")
    b = FederatedToken(idp_token_supplier=lambda: "tok")

    sdk.create_stream(table_properties=_props(), auth=a)
    sdk.create_stream(table_properties=_props(), auth=b)
    sdk.create_stream(table_properties=_props(), auth=a)

    key_a1, key_b, key_a2 = (fake.calls[0][4], fake.calls[1][4], fake.calls[2][4])
    assert key_a1 and key_b, "each FederatedToken must carry a cache_key"
    assert key_a1 != key_b, "distinct FederatedToken instances must isolate"
    assert key_a1 == key_a2, "reusing one FederatedToken must keep the same cache_key"


def test_create_stream_oauth_path_unchanged():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream("cid", "secret", _props())
    assert fake.calls[0][0] == "oauth"
    assert fake.calls[0][1] == "cid"


def test_create_stream_oauth_four_positional_unchanged():
    # Protects the released positional signature (client_id, client_secret,
    # table_properties, options): all four must keep binding as before.
    sdk, fake = _sync_sdk_with_fake()
    options = object()
    sdk.create_stream("cid", "secret", _props(), options)
    kind, client_id, client_secret, _tp, passed_options = fake.calls[0]
    assert (kind, client_id, client_secret) == ("oauth", "cid", "secret")
    assert passed_options is options


def test_auth_is_keyword_only():
    # auth is keyword-only: a positional (here 6th) argument is rejected, so it
    # can never be confused with the positional OAuth/headers parameters.
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(TypeError):
        sdk.create_stream("cid", "secret", _props(), None, None, FederatedToken(idp_token_supplier=lambda: "tok"))


def test_auth_with_positional_table_properties_raises_helpful_error():
    # The federation footgun: passing table_properties positionally lands it in
    # client_id. The client_id/auth conflict must raise a message naming the fix
    # rather than the misleading "table_properties is required".
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(ValueError, match="cannot be combined with auth="):
        sdk.create_stream(_props(), auth=FederatedToken(idp_token_supplier=lambda: "tok"))


def test_auth_takes_precedence_over_headers_provider():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok"),
        headers_provider=object(),
    )
    assert fake.calls[0][0] == "federated"


def test_create_stream_requires_auth_or_credentials():
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(ValueError):
        sdk.create_stream(table_properties=_props())


def test_create_stream_requires_table_properties():
    sdk, _fake = _sync_sdk_with_fake()
    with pytest.raises(ValueError):
        sdk.create_stream(auth=FederatedToken(idp_token_supplier=lambda: "tok"))


@pytest.mark.asyncio
async def test_async_create_stream_routes_to_federated():
    sdk = AsyncZerobusSdk(host="https://example", unity_catalog_url="https://example")
    fake = _AsyncFakeInner()
    sdk._inner = fake

    await sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp"),
    )
    assert fake.calls[0][0] == "federated"
    assert fake.calls[0][3] == "sp"


@pytest.mark.asyncio
async def test_async_auth_with_positional_table_properties_raises_helpful_error():
    sdk = AsyncZerobusSdk(host="https://example", unity_catalog_url="https://example")
    sdk._inner = _AsyncFakeInner()
    with pytest.raises(ValueError, match="cannot be combined with auth="):
        await sdk.create_stream(_props(), auth=FederatedToken(idp_token_supplier=lambda: "tok"))
