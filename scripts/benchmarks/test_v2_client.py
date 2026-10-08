import socket
import socketserver
import sys
import threading
import unittest
from pathlib import Path


SCRIPT_DIR = Path(__file__).resolve().parent
sys.path.insert(0, str(SCRIPT_DIR))

import v2_client  # noqa: E402


def frame(body: bytes) -> bytes:
    return len(body).to_bytes(4, "big") + body


def read_exact(stream: socket.socket, size: int) -> bytes:
    body = bytearray()
    while len(body) < size:
        chunk = stream.recv(size - len(body))
        if not chunk:
            raise AssertionError("client closed before expected bytes arrived")
        body.extend(chunk)
    return bytes(body)


def read_frame(stream: socket.socket, maximum: int) -> bytes:
    size = int.from_bytes(read_exact(stream, 4), "big")
    if not 0 < size <= maximum:
        raise AssertionError(f"unexpected client frame size {size}")
    return read_exact(stream, size)


def accepted_hello(*, auth_required: bool = False) -> bytes:
    accepted = (
        v2_client._varint_field(1, 2)
        + v2_client._varint_field(2, 0)
        + v2_client._varint_field(4, v2_client.MAX_CLIENT_TO_SERVER_BYTES)
        + v2_client._varint_field(5, v2_client.MAX_SERVER_TO_CLIENT_BYTES)
        + v2_client._varint_field(6, v2_client.MAX_CLIENT_TO_SERVER_BYTES)
        + v2_client._varint_field(7, v2_client.MAX_SERVER_TO_CLIENT_BYTES)
        + bytes([(8 << 3), int(auth_required)])
    )
    return v2_client._bytes_field(2, v2_client._bytes_field(1, accepted))


def response_frame(result_field: int, result: bytes) -> bytes:
    application = v2_client._bytes_field(result_field, result)
    return v2_client._bytes_field(3, application)


class _V2Server(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True


class V2ClientTests(unittest.TestCase):
    def test_negotiates_and_reuses_one_connection_for_binary_publish_and_poll(self) -> None:
        received: list[bytes] = []
        failure: list[BaseException] = []

        def handle(connection: socket.socket) -> None:
            try:
                self.assertEqual(read_exact(connection, len(v2_client.PREFACE)), v2_client.PREFACE)
                hello = read_frame(connection, v2_client.HELLO_MAX_BYTES)
                hello_fields = v2_client._parse_fields(hello)
                client_hello = v2_client._parse_fields(
                    v2_client._one(hello_fields, 1, 2, required=True)
                )
                version_range = v2_client._repeated(client_hello, 1, 2)
                self.assertEqual(len(version_range), 1)
                version_fields = v2_client._parse_fields(version_range[0])
                self.assertEqual(v2_client._one(version_fields, 1, 0), 2)
                connection.sendall(frame(accepted_hello()))

                create_request = read_frame(
                    connection, v2_client.MAX_CLIENT_TO_SERVER_BYTES
                )
                received.append(create_request)
                created = v2_client._string_field(1, "events") + v2_client._varint_field(2, 1)
                connection.sendall(frame(response_frame(3, created)))

                publish_request = read_frame(
                    connection, v2_client.MAX_CLIENT_TO_SERVER_BYTES
                )
                received.append(publish_request)
                published = v2_client._string_field(1, "events") + v2_client._varint_field(2, 0)
                connection.sendall(frame(response_frame(4, published)))

                poll_request = read_frame(
                    connection, v2_client.MAX_CLIENT_TO_SERVER_BYTES
                )
                received.append(poll_request)
                binary_payload = b"\x00\xffarbitrary\x80bytes"
                message = (
                    v2_client._string_field(1, "events")
                    + v2_client._string_field(2, "worker")
                    + v2_client._varint_field(4, 7)
                    + v2_client._bytes_field(6, binary_payload)
                )
                connection.sendall(frame(response_frame(7, message)))
            except BaseException as error:
                failure.append(error)

        with _V2Server(("127.0.0.1", 0), socketserver.BaseRequestHandler) as server:
            server.RequestHandlerClass = type(
                "Handler",
                (socketserver.BaseRequestHandler,),
                {"handle": lambda instance: handle(instance.request)},
            )
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            client = v2_client.V2Client("127.0.0.1", server.server_address[1])
            try:
                created, _ = client.request({"op": "create_stream", "stream": "events"})
                published, _ = client.request(
                    {"op": "publish", "stream": "events", "payload": b"\x00\xff"}
                )
                message, _ = client.request(
                    {"op": "poll", "stream": "events", "consumer": "worker"}
                )
            finally:
                client.close()
                server.shutdown()
                worker.join(timeout=2)

        self.assertFalse(failure, failure)
        self.assertEqual(len(received), 3)
        self.assertEqual(created["type"], "stream_created")
        self.assertEqual(published["offset"], 0)
        self.assertEqual(message["payload_bytes"], b"\x00\xffarbitrary\x80bytes")
        self.assertNotIn("payload", message)
        self.assertEqual(message["payload_hex"], "00ff617262697472617279806279746573")

    def test_auth_required_server_is_rejected_before_application_requests(self) -> None:
        observed: list[bytes] = []

        def handle(connection: socket.socket) -> None:
            observed.append(read_exact(connection, len(v2_client.PREFACE)))
            read_frame(connection, v2_client.HELLO_MAX_BYTES)
            connection.sendall(frame(accepted_hello(auth_required=True)))
            observed.append(connection.recv(1))

        with _V2Server(("127.0.0.1", 0), socketserver.BaseRequestHandler) as server:
            server.RequestHandlerClass = type(
                "Handler",
                (socketserver.BaseRequestHandler,),
                {"handle": lambda instance: handle(instance.request)},
            )
            worker = threading.Thread(target=server.serve_forever, daemon=True)
            worker.start()
            with self.assertRaisesRegex(
                v2_client.V2ProtocolError, "requires bearer authentication"
            ):
                v2_client.V2Client("127.0.0.1", server.server_address[1])
            server.shutdown()
            worker.join(timeout=2)

        self.assertEqual(observed, [v2_client.PREFACE, b""])

    def test_publish_batch_encodes_raw_payload_bytes(self) -> None:
        body = v2_client._encode_application_request(
            {
                "op": "publish_batch",
                "stream": "events",
                "records": [{"payload": b"\x00\xffbytes"}],
            }
        )
        frame_fields = v2_client._parse_fields(body)
        client_frame = v2_client._parse_fields(
            v2_client._one(frame_fields, 2, 2, required=True)
        )
        batch = v2_client._parse_fields(
            v2_client._one(client_frame, 3, 2, required=True)
        )
        record = v2_client._parse_fields(v2_client._repeated(batch, 2, 2)[0])
        self.assertEqual(v2_client._one(record, 2, 2), b"\x00\xffbytes")

    def test_frame_decoder_rejects_peer_limit_above_core_hard_cap(self) -> None:
        accepted = (
            v2_client._varint_field(1, 2)
            + v2_client._varint_field(2, 0)
            + v2_client._varint_field(4, v2_client.MAX_CLIENT_TO_SERVER_BYTES + 1)
            + v2_client._varint_field(5, v2_client.MAX_SERVER_TO_CLIENT_BYTES)
            + v2_client._varint_field(6, v2_client.MAX_CLIENT_TO_SERVER_BYTES)
            + v2_client._varint_field(7, v2_client.MAX_SERVER_TO_CLIENT_BYTES)
            + bytes([(8 << 3), 0])
        )
        with self.assertRaisesRegex(v2_client.V2ProtocolError, "unusable inbound frame limit"):
            v2_client._decode_server_hello(
                v2_client._bytes_field(2, v2_client._bytes_field(1, accepted))
            )

    def test_publish_batch_response_keeps_each_outcome(self) -> None:
        first = (
            v2_client._varint_field(1, 1)
            + v2_client._varint_field(2, 4)
            + v2_client._varint_field(3, 9)
        )
        second = (
            v2_client._varint_field(1, 1)
            + v2_client._varint_field(2, 4)
            + v2_client._varint_field(3, 10)
        )
        result = (
            v2_client._string_field(1, "events")
            + v2_client._bytes_field(2, first)
            + v2_client._bytes_field(2, second)
        )

        response = v2_client._decode_application_response(response_frame(5, result))

        self.assertEqual(response["type"], "publish_batch")
        self.assertEqual(
            [item["offset"] for item in response["outcomes"]], [9, 10]
        )


if __name__ == "__main__":
    unittest.main()
