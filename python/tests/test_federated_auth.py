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

    def create_stream_federated(self, table_properties, idp_token_supplier, databricks_client_id, options):
        self.calls.append(("federated", table_properties, idp_token_supplier, databricks_client_id, options))
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

    async def create_stream_federated(self, table_properties, idp_token_supplier, databricks_client_id, options):
        self.calls.append(("federated", table_properties, idp_token_supplier, databricks_client_id, options))
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
    kind, _tp, passed_supplier, client_id, _opts = fake.calls[0]
    assert kind == "federated"
    assert passed_supplier is supplier
    assert client_id is None


def test_create_stream_routes_auth_to_federated_workload():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream(
        table_properties=_props(),
        auth=FederatedToken(idp_token_supplier=lambda: "tok", databricks_client_id="sp-uuid"),
    )
    kind, _tp, _sup, client_id, _opts = fake.calls[0]
    assert kind == "federated"
    assert client_id == "sp-uuid"


def test_create_stream_oauth_path_unchanged():
    sdk, fake = _sync_sdk_with_fake()
    sdk.create_stream("cid", "secret", _props())
    assert fake.calls[0][0] == "oauth"
    assert fake.calls[0][1] == "cid"


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
