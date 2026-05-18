"""
Shared utilities for Zerobus SDK.

This module re-exports Rust core types and the new union-type selectors
(`OAuth`, `Headers`, `Format`).
"""

from zerobus._zerobus_core import (
    HeadersProvider,
    NonRetriableException,
    RecordType,
    ZerobusException,
)
from zerobus.sdk.shared.auth import Auth, Headers, OAuth
from zerobus.sdk.shared.config import AckCallback, StreamConfigurationOptions
from zerobus.sdk.shared.format import Format

__all__ = [
    "AckCallback",
    "HeadersProvider",
    "NonRetriableException",
    "RecordType",
    "StreamConfigurationOptions",
    "ZerobusException",
    "OAuth",
    "Headers",
    "Auth",
    "Format",
]
