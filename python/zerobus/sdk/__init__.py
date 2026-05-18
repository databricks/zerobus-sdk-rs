"""
Python SDK for the Zerobus Ingest API.

This is the synchronous version of the SDK. For the asynchronous version,
import from `zerobus.sdk.aio`.
"""

from zerobus._zerobus_core import (
    NonRetriableException,
    RecordType,
    StreamConfigurationOptions,
    ZerobusException,
)

from . import sync
from .shared.auth import Headers, OAuth
from .shared.format import Format

ZerobusSdk = sync.ZerobusSdk
ZerobusStream = sync.ZerobusStream

__all__ = [
    "ZerobusSdk",
    "ZerobusStream",
    "StreamConfigurationOptions",
    "RecordType",
    "ZerobusException",
    "NonRetriableException",
    "OAuth",
    "Headers",
    "Format",
]
