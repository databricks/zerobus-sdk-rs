"""
Federated external-IdP authentication for Zerobus streams.

This module defines :class:`FederatedToken`, the opt-in configuration for
authenticating a stream with an external identity provider (for example
Entra ID) instead of a Databricks OAuth client_id/client_secret.
"""

import uuid
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Optional, Union

# A zero-arg callback returning the current external IdP token (e.g. an Entra ID
# JWT), either synchronously (``str``) or asynchronously (an awaitable of
# ``str``).
IdpTokenSupplier = Callable[[], Union[str, Awaitable[str]]]


@dataclass
class FederatedToken:
    """Authenticate a Zerobus stream by federating an external IdP token, passed as
    the ``auth`` argument to ``create_stream``.

    Args:
        idp_token_supplier: A zero-arg callable returning the current external
            IdP token as a string, synchronous (``str``) or asynchronous (an
            awaitable of ``str``; async requires the async SDK). Called only when
            a fresh token must be minted, not on every request.
        databricks_client_id: The Databricks service principal client_id for
            workload identity federation, or ``None`` for account-level
            federation.
    """

    idp_token_supplier: IdpTokenSupplier
    databricks_client_id: Optional[str] = None

    # Stable per-instance key that partitions the SDK's shared token cache for
    # account-level federation, so two FederatedToken instances (different
    # identities) used from one ZerobusSdk do not collide and serve each other's
    # exchanged token. Reusing the same FederatedToken across streams keeps the
    # cache shared (same key); a fresh instance isolates. Not a constructor
    # argument: it is generated automatically and never needs to be set. Ignored
    # for workload identity federation, which the cache keys by client_id.
    _cache_key: str = field(default_factory=lambda: uuid.uuid4().hex, init=False, repr=False, compare=False)


__all__ = [
    "FederatedToken",
    "IdpTokenSupplier",
]
