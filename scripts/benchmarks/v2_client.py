"""Small persistent Protocol Buffers v2 client for repository workflows.

This is intentionally not a general SDK. It supports the scalar operations used
by benchmark and product-fit workloads, grouped delivery, replay, and publish
batches. TLS, bearer authentication, consume batches, acknowledgement batches,
consumer-policy operations, and compatibility with older protocols are not
implemented here.
"""

from __future__ import annotations

import socket
import time
from typing import Any


PREFACE = b"RNLN\x01\x00\x00\x00"
CURRENT_MAJOR = 2
CURRENT_MINOR = 0
HELLO_MAX_BYTES = 16 * 1024
AUTH_MAX_BYTES = 1024
MIN_FRAME_BYTES = 1024
MAX_CLIENT_TO_SERVER_BYTES = 64 * 1024 * 1024
MAX_SERVER_TO_CLIENT_BYTES = 65 * 1024 * 1024
DEFAULT_CLIENT_TO_SERVER_BYTES = MAX_CLIENT_TO_SERVER_BYTES
DEFAULT_SERVER_TO_CLIENT_BYTES = MAX_SERVER_TO_CLIENT_BYTES
MAX_PUBLISH_BATCH_RECORDS = 1024
MAX_UINT32 = (1 << 32) - 1
MAX_UINT64 = (1 << 64) - 1


class V2ProtocolError(RuntimeError):
    """A transport, framing, negotiation, or supported-operation failure."""


def _encode_varint(value: int) -> bytes:
    if type(value) is not int or value < 0 or value > MAX_UINT64:
        raise V2ProtocolError("Protobuf integer is outside the uint64 range")
    encoded = bytearray()
    while value >= 0x80:
        encoded.append((value & 0x7F) | 0x80)
        value >>= 7
    encoded.append(value)
    return bytes(encoded)


def _varint_field(number: int, value: int, *, present: bool = True) -> bytes:
    if not present or value == 0:
        return b""
    return _encode_varint(number << 3) + _encode_varint(value)


def _bytes_field(number: int, value: bytes) -> bytes:
    return _encode_varint((number << 3) | 2) + _encode_varint(len(value)) + value


def _string_field(number: int, value: str | None, *, optional: bool = False) -> bytes:
    if value is None:
        if optional:
            return b""
        raise V2ProtocolError(f"missing required string field {number}")
    if not isinstance(value, str):
        raise V2ProtocolError(f"string field {number} must be text")
    encoded = value.encode("utf-8")
    if optional and value == "":
        return _bytes_field(number, encoded)
    if not encoded:
        return b""
    return _bytes_field(number, encoded)


def _uint(value: Any, maximum: int, name: str) -> int:
    if type(value) is not int or value < 0 or value > maximum:
        raise V2ProtocolError(f"{name} must be an unsigned integer no larger than {maximum}")
    return value


def _read_varint(body: bytes, cursor: int) -> tuple[int, int]:
    value = 0
    for shift in range(0, 70, 7):
        if cursor >= len(body):
            raise V2ProtocolError("truncated Protobuf varint")
        byte = body[cursor]
        cursor += 1
        if shift == 63 and byte > 1:
            raise V2ProtocolError("Protobuf varint exceeds uint64")
        value |= (byte & 0x7F) << shift
        if byte & 0x80 == 0:
            return value, cursor
    raise V2ProtocolError("malformed Protobuf varint")


def _parse_fields(body: bytes) -> list[tuple[int, int, int | bytes]]:
    fields: list[tuple[int, int, int | bytes]] = []
    cursor = 0
    while cursor < len(body):
        key, cursor = _read_varint(body, cursor)
        number, wire_type = key >> 3, key & 7
        if number == 0 or number > 0x1FFF_FFFF:
            raise V2ProtocolError("invalid Protobuf field number")
        if wire_type == 0:
            value, cursor = _read_varint(body, cursor)
        elif wire_type == 1:
            end = cursor + 8
            if end > len(body):
                raise V2ProtocolError("truncated Protobuf fixed64 field")
            value = body[cursor:end]
            cursor = end
        elif wire_type == 2:
            size, cursor = _read_varint(body, cursor)
            end = cursor + size
            if end > len(body):
                raise V2ProtocolError("truncated Protobuf byte field")
            value = body[cursor:end]
            cursor = end
        elif wire_type == 5:
            end = cursor + 4
            if end > len(body):
                raise V2ProtocolError("truncated Protobuf fixed32 field")
            value = body[cursor:end]
            cursor = end
        else:
            raise V2ProtocolError("unsupported Protobuf wire type")
        fields.append((number, wire_type, value))
    return fields


def _one(
    fields: list[tuple[int, int, int | bytes]],
    number: int,
    wire_type: int,
    *,
    required: bool = False,
    default: int | bytes | None = None,
) -> int | bytes | None:
    matches = [(kind, value) for field, kind, value in fields if field == number]
    if not matches:
        if required:
            raise V2ProtocolError(f"missing Protobuf field {number}")
        return default
    if len(matches) != 1 or matches[0][0] != wire_type:
        raise V2ProtocolError(f"invalid or duplicate Protobuf field {number}")
    return matches[0][1]


def _repeated(
    fields: list[tuple[int, int, int | bytes]], number: int, wire_type: int
) -> list[int | bytes]:
    matches = [(kind, value) for field, kind, value in fields if field == number]
    if any(kind != wire_type for kind, _ in matches):
        raise V2ProtocolError(f"invalid Protobuf field {number}")
    return [value for _, value in matches]


def _text(value: int | bytes | None, name: str) -> str:
    if not isinstance(value, bytes):
        raise V2ProtocolError(f"invalid Protobuf text field {name}")
    try:
        return value.decode("utf-8")
    except UnicodeDecodeError as error:
        raise V2ProtocolError(f"invalid UTF-8 in Protobuf field {name}") from error


def _encode_client_hello() -> bytes:
    version = _varint_field(1, CURRENT_MAJOR)
    client_hello = _bytes_field(1, version)
    client_hello += _varint_field(4, DEFAULT_CLIENT_TO_SERVER_BYTES)
    client_hello += _varint_field(5, DEFAULT_SERVER_TO_CLIENT_BYTES)
    return _bytes_field(1, client_hello)


def _decode_server_hello(body: bytes) -> tuple[int, int]:
    frame = _parse_fields(body)
    selected = [(number, value) for number, kind, value in frame if number in (1, 2) and kind == 2]
    if len(selected) != 1:
        raise V2ProtocolError("server Hello has no unique recognized message")
    if selected[0][0] != 2:
        raise V2ProtocolError("server replied with a client Hello")
    server_hello = _parse_fields(selected[0][1])  # type: ignore[arg-type]
    result = [(number, value) for number, kind, value in server_hello if number in (1, 2) and kind == 2]
    if len(result) != 1:
        raise V2ProtocolError("server Hello has no unique result")
    if result[0][0] == 2:
        refusal = _parse_fields(result[0][1])  # type: ignore[arg-type]
        code = _one(refusal, 1, 0, required=True)
        diagnostic = _text(_one(refusal, 2, 2, default=b""), "refusal diagnostic")
        raise V2ProtocolError(f"server refused protocol v2 negotiation ({code}): {diagnostic}")

    accepted = _parse_fields(result[0][1])  # type: ignore[arg-type]
    major = _uint(_one(accepted, 1, 0, required=True), MAX_UINT32, "major")
    minor = _uint(_one(accepted, 2, 0, required=True), MAX_UINT32, "minor")
    capabilities = [_text(value, "capabilities") for value in _repeated(accepted, 3, 2)]
    server_inbound = _uint(
        _one(accepted, 4, 0, required=True), MAX_UINT32, "server inbound limit"
    )
    server_outbound = _uint(
        _one(accepted, 5, 0, required=True), MAX_UINT32, "server outbound limit"
    )
    client_to_server = _uint(
        _one(accepted, 6, 0, required=True), MAX_UINT32, "client-to-server limit"
    )
    server_to_client = _uint(
        _one(accepted, 7, 0, required=True), MAX_UINT32, "server-to-client limit"
    )
    auth_required = _one(accepted, 8, 0, required=True)
    if auth_required not in (0, 1):
        raise V2ProtocolError("server Hello auth_required is not a boolean")
    if major != CURRENT_MAJOR or minor != CURRENT_MINOR:
        raise V2ProtocolError(f"server selected unsupported protocol {major}.{minor}")
    if capabilities:
        raise V2ProtocolError("server selected a capability the workflow client did not offer")
    if not MIN_FRAME_BYTES <= server_inbound <= MAX_CLIENT_TO_SERVER_BYTES:
        raise V2ProtocolError("server advertised an unusable inbound frame limit")
    if not MIN_FRAME_BYTES <= server_outbound <= MAX_SERVER_TO_CLIENT_BYTES:
        raise V2ProtocolError("server advertised an unusable outbound frame limit")
    expected_client_to_server = min(DEFAULT_CLIENT_TO_SERVER_BYTES, server_inbound)
    expected_server_to_client = min(DEFAULT_SERVER_TO_CLIENT_BYTES, server_outbound)
    if client_to_server != expected_client_to_server or server_to_client != expected_server_to_client:
        raise V2ProtocolError("server Hello returned inconsistent negotiated limits")
    if not MIN_FRAME_BYTES <= client_to_server <= MAX_CLIENT_TO_SERVER_BYTES:
        raise V2ProtocolError("server selected an unusable client-to-server limit")
    if not MIN_FRAME_BYTES <= server_to_client <= MAX_SERVER_TO_CLIENT_BYTES:
        raise V2ProtocolError("server selected an unusable server-to-client limit")
    if auth_required:
        raise V2ProtocolError("server requires bearer authentication, unsupported by workflow client")
    return client_to_server, server_to_client


def _payload_fields(payload: bytes) -> dict[str, Any]:
    result: dict[str, Any] = {"payload_bytes": payload}
    try:
        result["payload"] = payload.decode("utf-8")
    except UnicodeDecodeError:
        result["payload_hex"] = payload.hex()
    return result


def _encode_application_request(request: dict[str, Any]) -> bytes:
    if not isinstance(request, dict):
        raise V2ProtocolError("application request must be a mapping")
    operation = request.get("op")
    stream = request.get("stream")
    consumer = request.get("consumer")
    member = request.get("member")
    if operation == "create_stream":
        number, message = 1, _string_field(1, stream)
    elif operation == "publish":
        payload = request.get("payload")
        if isinstance(payload, str):
            payload = payload.encode("utf-8")
        if not isinstance(payload, bytes):
            raise V2ProtocolError("publish payload must be text or bytes")
        message = _string_field(1, stream)
        message += _string_field(2, request.get("key"), optional=True)
        message += _bytes_field(3, payload)
        message += _string_field(4, request.get("request_id"), optional=True)
        number = 2
    elif operation == "publish_batch":
        records = request.get("records")
        if not isinstance(records, list) or not 1 <= len(records) <= MAX_PUBLISH_BATCH_RECORDS:
            raise V2ProtocolError("publish_batch requires 1 through 1024 records")
        message = _string_field(1, stream)
        for record in records:
            if not isinstance(record, dict):
                raise V2ProtocolError("publish_batch records must be mappings")
            payload = record.get("payload")
            if isinstance(payload, str):
                payload = payload.encode("utf-8")
            if not isinstance(payload, bytes):
                raise V2ProtocolError("publish_batch payload must be text or bytes")
            item = _string_field(1, record.get("key"), optional=True)
            item += _bytes_field(2, payload)
            item += _string_field(3, record.get("request_id"), optional=True)
            message += _bytes_field(2, item)
        number = 3
    elif operation == "poll":
        number, message = 4, _string_field(1, stream) + _string_field(2, consumer)
    elif operation == "replay":
        offset = _uint(request.get("offset"), MAX_UINT64, "offset")
        number = 6
        message = _string_field(1, stream) + _string_field(2, consumer)
        message += _varint_field(3, offset)
    elif operation == "poll_group":
        number = 7
        message = (
            _string_field(1, stream)
            + _string_field(2, consumer)
            + _string_field(3, member)
        )
    elif operation == "ack":
        offset = _uint(request.get("offset"), MAX_UINT64, "offset")
        number = 11
        message = (
            _string_field(1, stream)
            + _string_field(2, consumer)
            + _varint_field(3, offset)
        )
    elif operation == "ack_group":
        offset = _uint(request.get("offset"), MAX_UINT64, "offset")
        number = 13
        message = (
            _string_field(1, stream)
            + _string_field(2, consumer)
            + _string_field(3, member)
            + _varint_field(4, offset)
            + _string_field(5, request.get("delivery_token"))
        )
    else:
        raise V2ProtocolError(f"unsupported workflow operation: {operation!r}")
    application_request = _bytes_field(number, message)
    application = _bytes_field(1, application_request)
    return _bytes_field(2, application)


def _decode_message(fields: list[tuple[int, int, int | bytes]]) -> dict[str, Any]:
    result: dict[str, Any] = {"type": "message"}
    for number, name in ((1, "stream"), (2, "consumer"), (3, "member"), (5, "key"), (8, "delivery_token")):
        value = _one(fields, number, 2)
        if value is not None:
            result[name] = _text(value, name)
    result["offset"] = _uint(_one(fields, 4, 0, default=0), MAX_UINT64, "offset")
    payload = _one(fields, 6, 2, default=b"")
    result.update(_payload_fields(payload))  # type: ignore[arg-type]
    result["published_at_ms"] = _uint(
        _one(fields, 7, 0, default=0), MAX_UINT64, "published_at_ms"
    )
    attempt = _one(fields, 9, 0)
    if attempt is not None:
        result["delivery_attempt"] = _uint(attempt, MAX_UINT32, "delivery_attempt")
    return result


def _decode_application_response(body: bytes) -> dict[str, Any]:
    frame = _parse_fields(body)
    apps = [(kind, value) for number, kind, value in frame if number == 3]
    if len(apps) != 1 or apps[0][0] != 2 or not isinstance(apps[0][1], bytes):
        raise V2ProtocolError("server frame did not contain one application response")
    response = _parse_fields(apps[0][1])
    outcome = _one(response, 1, 0)
    stage = _one(response, 2, 0)
    results = [(number, value) for number, kind, value in response if number in range(3, 15) and kind == 2]
    if len(results) != 1 or not isinstance(results[0][1], bytes):
        raise V2ProtocolError("application response did not contain one recognized result")
    kind, payload = results[0]
    fields = _parse_fields(payload)

    if kind == 3:
        return {
            "type": "stream_created",
            "stream": _text(_one(fields, 1, 2, default=b""), "stream"),
            "created": bool(_one(fields, 2, 0, default=0)),
        }
    if kind == 4:
        return {
            "type": "published",
            "stream": _text(_one(fields, 1, 2, default=b""), "stream"),
            "offset": _uint(_one(fields, 2, 0, default=0), MAX_UINT64, "offset"),
        }
    if kind == 5:
        stream = _text(_one(fields, 1, 2, default=b""), "stream")
        outcomes: list[dict[str, Any]] = []
        for item in _repeated(fields, 2, 2):
            item_fields = _parse_fields(item)  # type: ignore[arg-type]
            item_outcome = _one(item_fields, 1, 0, required=True)
            item_stage = _one(item_fields, 2, 0, required=True)
            offset = _one(item_fields, 3, 0)
            if offset is not None:
                outcomes.append({
                    "type": "published",
                    "offset": _uint(offset, MAX_UINT64, "offset"),
                    "outcome": item_outcome,
                    "stage": item_stage,
                })
            else:
                outcomes.append({
                    "type": "error",
                    "code": _text(_one(item_fields, 4, 2, required=True), "code"),
                    "message": _text(_one(item_fields, 5, 2, required=True), "diagnostic"),
                    "outcome": item_outcome,
                    "stage": item_stage,
                })
        return {"type": "publish_batch", "stream": stream, "outcomes": outcomes}
    if kind == 7:
        return _decode_message(fields)
    if kind == 8:
        result = {
            "type": "replay_message",
            "stream": _text(_one(fields, 1, 2, default=b""), "stream"),
            "consumer": _text(_one(fields, 2, 2, default=b""), "consumer"),
            "offset": _uint(_one(fields, 3, 0, default=0), MAX_UINT64, "offset"),
            "published_at_ms": _uint(_one(fields, 6, 0, default=0), MAX_UINT64, "published_at_ms"),
        }
        key = _one(fields, 4, 2)
        if key is not None:
            result["key"] = _text(key, "key")
        result.update(_payload_fields(_one(fields, 5, 2, default=b"")))  # type: ignore[arg-type]
        return result
    if kind == 9:
        return {
            "type": "empty",
            "stream": _text(_one(fields, 1, 2, default=b""), "stream"),
            "consumer": _text(_one(fields, 2, 2, default=b""), "consumer"),
        }
    if kind == 10:
        return {
            "type": "acknowledged",
            "stream": _text(_one(fields, 1, 2, default=b""), "stream"),
            "consumer": _text(_one(fields, 2, 2, default=b""), "consumer"),
            "offset": _uint(_one(fields, 3, 0, default=0), MAX_UINT64, "offset"),
            "already_acknowledged": bool(_one(fields, 4, 0, default=0)),
        }
    if kind == 14:
        result = {
            "type": "error",
            "code": _text(_one(fields, 1, 2, required=True), "code"),
            "message": _text(_one(fields, 2, 2, required=True), "diagnostic"),
        }
        if outcome is not None:
            result["outcome"] = outcome
        if stage is not None:
            result["stage"] = stage
        return result
    raise V2ProtocolError(f"unsupported workflow response variant {kind}")


class V2Client:
    """Synchronous sequential v2 client for one persistent TCP connection."""

    def __init__(
        self,
        host: str,
        port: int,
        timeout_seconds: float = 30.0,
    ) -> None:
        try:
            self.socket = socket.create_connection((host, port), timeout=timeout_seconds)
            self.socket.settimeout(timeout_seconds)
            self.max_outbound_bytes, self.max_inbound_bytes = self._negotiate()
        except (OSError, V2ProtocolError) as error:
            socket_value = getattr(self, "socket", None)
            if socket_value is not None:
                socket_value.close()
            if isinstance(error, V2ProtocolError):
                raise
            raise V2ProtocolError(f"could not connect to v2 broker: {error}") from error

    def _receive_exact(self, size: int) -> bytes:
        received = bytearray()
        while len(received) < size:
            chunk = self.socket.recv(size - len(received))
            if not chunk:
                raise V2ProtocolError("broker closed the v2 connection")
            received.extend(chunk)
        return bytes(received)

    def _receive_frame(self, maximum: int) -> bytes:
        size = int.from_bytes(self._receive_exact(4), "big")
        if size == 0 or size > maximum:
            raise V2ProtocolError(f"broker frame size {size} is outside the limit {maximum}")
        return self._receive_exact(size)

    def _send_frame(self, body: bytes, maximum: int) -> None:
        if not body or len(body) > maximum or len(body) > MAX_UINT32:
            raise V2ProtocolError("outbound frame exceeds its negotiated limit")
        self.socket.sendall(len(body).to_bytes(4, "big") + body)

    def _negotiate(self) -> tuple[int, int]:
        self.socket.sendall(PREFACE)
        self._send_frame(_encode_client_hello(), HELLO_MAX_BYTES)
        body = self._receive_frame(HELLO_MAX_BYTES)
        client_to_server, server_to_client = _decode_server_hello(body)
        return client_to_server, server_to_client

    def request(self, request: dict[str, Any]) -> tuple[dict[str, Any], int]:
        body = _encode_application_request(request)
        if len(body) > self.max_outbound_bytes:
            raise V2ProtocolError("outbound application request exceeds negotiated limit")
        started = time.perf_counter_ns()
        try:
            self._send_frame(body, self.max_outbound_bytes)
            response_body = self._receive_frame(self.max_inbound_bytes)
            elapsed_ns = time.perf_counter_ns() - started
            frame = _parse_fields(response_body)
            variants = [(number, value) for number, wire, value in frame if number in (1, 2, 3) and wire == 2]
            if len(variants) != 1:
                raise V2ProtocolError("server frame did not contain one recognized response")
            if variants[0][0] == 1:
                raise V2ProtocolError("unexpected authenticated control frame")
            if variants[0][0] == 2:
                raise V2ProtocolError("server rejected bearer authentication")
            response = _decode_application_response(response_body)
        except (OSError, V2ProtocolError) as error:
            self.close()
            if isinstance(error, V2ProtocolError):
                raise
            raise V2ProtocolError(f"v2 request failed: {error}") from error
        return response, elapsed_ns

    def close(self) -> None:
        try:
            self.socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self.socket.close()
