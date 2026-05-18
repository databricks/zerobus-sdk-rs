"""
Authentication selectors for stream creation.

The Python SDK uses tagged union types (frozen dataclasses) to pick how a
stream authenticates. New auth strategies can be added in future releases by
introducing new dataclasses — existing call sites that pass `OAuth(...)` or
`Headers(...)` remain unchanged.

Example:
    from zerobus import ZerobusSdk, OAuth, Format
    sdk = ZerobusSdk(host=..., unity_catalog_url=...)
    stream = sdk.create_stream(
        table=..., auth=OAuth("id", "secret"), record_format=Format.JSON
    )

    # Or with a custom HeadersProvider:
    from zerobus import Headers, HeadersProvider
    class MyHeaders(HeadersProvider):
        def get_headers(self):
            return [("authorization", "Bearer ...")]
    stream = sdk.create_stream(
        table=..., auth=Headers(MyHeaders()), record_format=Format.JSON
    )
"""

from dataclasses import dataclass, field
from typing import Union

from zerobus._zerobus_core import HeadersProvider


@dataclass(frozen=True)
class OAuth:
    """OAuth client credentials. The Rust core fetches and refreshes tokens internally.

    `client_secret` is excluded from `__repr__` to keep credentials out of
    logs and stack traces.
    """

    client_id: str
    client_secret: str = field(repr=False)


@dataclass(frozen=True)
class Headers:
    """Authenticate via a custom HeadersProvider subclass."""

    provider: HeadersProvider

    def __post_init__(self):
        if not isinstance(self.provider, HeadersProvider):
            raise TypeError(
                "Headers(provider=...) requires a HeadersProvider subclass; " f"got {type(self.provider).__name__}"
            )


Auth = Union[OAuth, Headers]


__all__ = ["OAuth", "Headers", "Auth"]
