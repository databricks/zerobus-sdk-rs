"""
Shared utilities for Zerobus SDK.

This module provides Python wrappers around Rust types with comprehensive
documentation and type hints.
"""

# Re-export Rust types that don't need wrappers
from zerobus._zerobus_core import (
    HeadersProvider,
    NonRetriableException,
    RecordType,
    TableProperties,
    ZerobusException,
)

# Import Python wrappers with documentation
from zerobus.sdk.shared.auth import FederatedToken, IdpTokenSupplier
from zerobus.sdk.shared.config import AckCallback, StreamConfigurationOptions

__all__ = [
    "AckCallback",
    "FederatedToken",
    "HeadersProvider",
    "IdpTokenSupplier",
    "NonRetriableException",
    "RecordType",
    "StreamConfigurationOptions",
    "TableProperties",
    "ZerobusException",
]
