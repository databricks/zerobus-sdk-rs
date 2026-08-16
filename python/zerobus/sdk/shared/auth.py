"""
Federated external-IdP authentication for Zerobus streams.

This module defines :class:`FederatedToken`, the opt-in configuration for
authenticating a stream with an external identity provider (for example
Entra ID) instead of a Databricks OAuth client_id/client_secret.
"""

from dataclasses import dataclass
from typing import Awaitable, Callable, Optional, Union

# A zero-arg callback returning the current external IdP token (e.g. an Entra ID
# JWT), either synchronously (``str``) or asynchronously (an awaitable of
# ``str``).
IdpTokenSupplier = Callable[[], Union[str, Awaitable[str]]]


@dataclass
class FederatedToken:
    """Authenticate a Zerobus stream by federating an external IdP token.

    The SDK exchanges the external IdP token returned by ``idp_token_supplier``
    for a Zerobus-scoped Databricks token via RFC 8693 token exchange. The
    exchange happens client-side, in the SDK; the Zerobus service is unchanged.

    Two federation modes are selected by ``databricks_client_id``:

    * **Account-level federation** (``databricks_client_id=None``): no
      Databricks-managed service principal. The token subject is resolved to an
      identity synced into Databricks via Automatic Identity Management (SCIM).
    * **Workload identity federation** (``databricks_client_id`` set): a
      Databricks service principal with a client_id and no secret, with a
      federation policy attached. The exchange names the service principal via
      its client_id.

    Pass an instance as the ``auth`` argument to ``create_stream``.

    Args:
        idp_token_supplier: A zero-arg callable returning the current external
            IdP token as a string. May be synchronous (returns ``str``) or
            asynchronous (returns an awaitable of ``str``); async suppliers
            require the async SDK. It is called only when a fresh Databricks
            token must be minted (a cache miss or refresh), never on every
            request, so a callable that fetches a token is fine here.
        databricks_client_id: The Databricks service principal client_id for
            workload identity federation, or ``None`` for account-level
            federation.
    """

    idp_token_supplier: IdpTokenSupplier
    databricks_client_id: Optional[str] = None


__all__ = [
    "FederatedToken",
    "IdpTokenSupplier",
]
