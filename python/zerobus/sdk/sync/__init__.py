"""
Sync Python SDK for the Zerobus service (Rust-backed).

This module provides high-performance synchronous ingestion backed by a Rust core.
"""

from zerobus.sdk.sync.zerobus_sdk import (
    HeadersProvider,
    NonRetriableException,
    RecordAcknowledgment,
    RecordType,
    StreamConfigurationOptions,
    TableProperties,
    ZerobusArrowStream,
    ZerobusException,
    ZerobusSdk,
    ZerobusStream,
)

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "ZerobusArrowStream",
    "RecordAcknowledgment",
    "TableProperties",
    "StreamConfigurationOptions",
    "RecordType",
    "HeadersProvider",
    "ZerobusException",
    "NonRetriableException",
]
