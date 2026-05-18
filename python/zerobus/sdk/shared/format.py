"""
Record-format selectors for stream creation.

`Format` is a tagged union — `Format.JSON` for JSON ingestion, `Format.proto(...)`
for protobuf ingestion. New formats can be added in future releases by
introducing new sentinel objects or factory methods; existing call sites that
pass `Format.JSON` or `Format.proto(desc)` remain unchanged.

Example:
    from zerobus import Format
    # JSON
    stream = sdk.create_stream(table=..., auth=..., record_format=Format.JSON)
    # Protobuf — pass a generated descriptor or its serialized bytes
    from my_proto_pb2 import MyMessage
    stream = sdk.create_stream(
        table=..., auth=..., record_format=Format.proto(MyMessage.DESCRIPTOR)
    )
"""

from dataclasses import dataclass
from typing import Any, Optional, Tuple, Union


@dataclass(frozen=True)
class _Json:
    """Marker for JSON record format. Use `Format.JSON`."""

    def __repr__(self) -> str:
        return "Format.JSON"


@dataclass(frozen=True)
class _Proto:
    """Marker for compiled-protobuf record format. Use `Format.proto(descriptor)`.

    `descriptor` is the original Python descriptor (or raw bytes) passed in by
    the caller. `message_name`, if set, narrows selection within a multi-
    message `FileDescriptorProto` to the message of that name.
    """

    descriptor: Any
    message_name: Optional[str] = None

    def __repr__(self) -> str:
        return f"Format.proto({type(self.descriptor).__name__})"


class Format:
    """Tagged-union of supported record formats.

    `Format.JSON` is a singleton sentinel; `Format.proto(descriptor)` constructs
    a proto variant. The descriptor may be a `bytes` of a serialized
    `FileDescriptorProto`, or a `google.protobuf.descriptor.Descriptor` object
    (the SDK reads its `file.serialized_pb` and `name`).
    """

    JSON: "_Json" = _Json()

    @staticmethod
    def proto(descriptor: Any) -> "_Proto":
        if descriptor is None:
            raise ValueError("Format.proto() requires a descriptor")
        # If the caller passed a Descriptor object we can extract its message
        # name eagerly and surface a clear error here on a malformed input.
        # Raw bytes fall back to "first message in file" on the Rust side.
        message_name = getattr(descriptor, "name", None)
        if isinstance(descriptor, (bytes, bytearray)):
            message_name = None
        return _Proto(descriptor=descriptor, message_name=message_name)


FormatSpec = Union[_Json, _Proto]


def _descriptor_to_bytes(descriptor: Any) -> Tuple[bytes, Optional[str]]:
    """Coerce a descriptor argument into raw `FileDescriptorProto` bytes and an
    optional message-name selector.

    Returns `(bytes, message_name)`. The message name is `None` when the caller
    handed in raw bytes (we cannot know which message they meant without the
    Descriptor wrapper).
    """
    if isinstance(descriptor, (bytes, bytearray)):
        return bytes(descriptor), None
    file_attr = getattr(descriptor, "file", None)
    if file_attr is None:
        raise TypeError(
            "Format.proto() expects bytes (serialized FileDescriptorProto) or a "
            "Descriptor object with file.serialized_pb"
        )
    serialized = getattr(file_attr, "serialized_pb", None)
    if serialized is None:
        raise TypeError("Descriptor.file does not have serialized_pb attribute")
    name = getattr(descriptor, "name", None)
    return bytes(serialized), name


__all__ = ["Format", "FormatSpec"]
