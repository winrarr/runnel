from __future__ import annotations

import json
import shutil
import socket
import socketserver
import ssl
import struct
import subprocess
import tempfile
import threading
import time
import unittest
from pathlib import Path
from unittest.mock import Mock

from cluster_faults import PeerResponseDelayProxy
import peer_tls


class PeerTlsCredentialsTests(unittest.TestCase):
    def test_response_delay_proxy_rejects_partial_tls_configuration(self) -> None:
        with self.assertRaisesRegex(ValueError, "configured together"):
            PeerResponseDelayProxy(
                0,
                0,
                peer_credentials=Mock(spec=peer_tls.PeerCredentials),
            )

        with self.assertRaisesRegex(ValueError, "configured together"):
            PeerResponseDelayProxy(0, 0, target_node_id=2)

    def test_generated_leaf_is_ca_signed_and_bound_to_node_and_cluster(self) -> None:
        if shutil.which("openssl") is None:
            self.fail("openssl is required by the clustered benchmark runner")

        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            credentials = peer_tls.PeerCredentials(root, "events", [1, 2])
            node = credentials.node(2)

            verified = subprocess.run(
                [
                    "openssl",
                    "verify",
                    "-CAfile",
                    str(node.trust_bundle),
                    str(node.certificate_chain),
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn("OK", verified.stdout)

            identity = subprocess.run(
                [
                    "openssl",
                    "x509",
                    "-in",
                    str(node.certificate_chain),
                    "-noout",
                    "-ext",
                    "subjectAltName",
                ],
                check=True,
                capture_output=True,
                text=True,
            )
            self.assertIn(peer_tls.peer_identity(2, "events"), identity.stdout)
            self.assertTrue(node.private_key.is_file())
            self.assertEqual(node.private_key.stat().st_mode & 0o777, 0o600)
            self.assertNotIn("ca-private.pem", {path.name for path in root.rglob("*")})

    def test_response_delay_proxy_preserves_peer_tls_identity(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            root = Path(temporary_directory)
            credentials = peer_tls.PeerCredentials(root, "events", [1, 2])
            node = credentials.node(2)
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.minimum_version = ssl.TLSVersion.TLSv1_3
            server_context.maximum_version = ssl.TLSVersion.TLSv1_3
            server_context.verify_mode = ssl.CERT_REQUIRED
            server_context.load_verify_locations(cafile=str(node.trust_bundle))
            server_context.load_cert_chain(
                certfile=str(node.certificate_chain),
                keyfile=str(node.private_key),
            )

            class EchoHandler(socketserver.BaseRequestHandler):
                def handle(self) -> None:
                    with server_context.wrap_socket(self.request, server_side=True) as stream:
                        self.server.peer_certificate = stream.getpeercert()
                        header = _receive_exact(stream, 4)
                        if header is None:
                            raise ConnectionError("proxy closed before sending a request")
                        payload_size = struct.unpack("!I", header)[0]
                        _receive_exact(stream, payload_size)
                        response = json.dumps({"Forward": {"ok": True}}).encode()
                        stream.sendall(struct.pack("!I", len(response)) + response)

            backend = socketserver.ThreadingTCPServer(("127.0.0.1", 0), EchoHandler)
            backend.allow_reuse_address = True
            backend.daemon_threads = True
            backend.peer_certificate = None
            backend_thread = threading.Thread(target=backend.serve_forever, daemon=True)
            backend_thread.start()
            proxy = PeerResponseDelayProxy(
                backend.server_address[1],
                30,
                credentials,
                target_node_id=2,
            )
            proxy.start()
            try:
                client_context = ssl.create_default_context(
                    ssl.Purpose.SERVER_AUTH,
                    cafile=str(node.trust_bundle),
                )
                client_context.minimum_version = ssl.TLSVersion.TLSv1_3
                client_context.maximum_version = ssl.TLSVersion.TLSv1_3
                client_node = credentials.node(1)
                client_context.load_cert_chain(
                    certfile=str(client_node.certificate_chain),
                    keyfile=str(client_node.private_key),
                )
                started = time.monotonic()
                with socket.socket() as raw_client:
                    raw_client.settimeout(5)
                    raw_client.connect(("127.0.0.1", proxy.port))
                    with client_context.wrap_socket(
                        raw_client,
                        server_hostname=peer_tls.peer_identity(2, "events"),
                    ) as client:
                        payload = json.dumps({"Forward": {"request": True}}).encode()
                        client.sendall(struct.pack("!I", len(payload)) + payload)
                        header = _receive_exact(client, 4)
                        if header is None:
                            self.fail("proxy closed before returning the response")
                        response_size = struct.unpack("!I", header)[0]
                        response = json.loads(_receive_exact(client, response_size))
                self.assertEqual(response, {"Forward": {"ok": True}})
                self.assertGreaterEqual(time.monotonic() - started, 0.03)
                self.assertEqual(
                    backend.peer_certificate["subjectAltName"],
                    (("DNS", peer_tls.peer_identity(1, "events")),),
                )
                self.assertEqual(proxy.summary()["delayed_responses"], 1)
            finally:
                proxy.close()
                backend.shutdown()
                backend.server_close()
                backend_thread.join(timeout=5)


def _receive_exact(stream: socket.socket | ssl.SSLSocket, size: int) -> bytes:
    data = bytearray()
    while len(data) < size:
        chunk = stream.recv(size - len(data))
        if not chunk:
            raise ConnectionError("peer test connection closed before a full frame")
        data.extend(chunk)
    return bytes(data)


if __name__ == "__main__":
    unittest.main()
