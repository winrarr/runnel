#!/usr/bin/env python3
"""Bounded fault-injection primitives for the clustered benchmark."""

from __future__ import annotations

import json
import ssl
import socket
import socketserver
import threading
import time
from typing import Any

from common import BenchmarkError, DEFAULT_TIMEOUT_SECONDS
from peer_tls import PeerCredentials, peer_identity


def _receive_exact(sock: socket.socket, size: int) -> bytes | None:
    chunks: list[bytes] = []
    remaining = size
    while remaining:
        chunk = sock.recv(remaining)
        if not chunk:
            if chunks:
                raise ConnectionError("peer proxy closed a partial frame")
            return None
        chunks.append(chunk)
        remaining -= len(chunk)
    return b"".join(chunks)


def _read_proxy_frame(sock: socket.socket) -> bytes | None:
    header = _receive_exact(sock, 4)
    if header is None:
        return None
    length = int.from_bytes(header, "big")
    if length > 64 * 1024 * 1024:
        raise BenchmarkError("peer proxy frame exceeds the 64 MiB limit")
    payload = _receive_exact(sock, length)
    if payload is None:
        raise ConnectionError("peer proxy closed a partial frame")
    return header + payload


def _is_forward_response(frame: bytes) -> bool:
    try:
        response = json.loads(frame[4:])
    except (UnicodeDecodeError, json.JSONDecodeError):
        return False
    return isinstance(response, dict) and "Forward" in response


class _PeerDelayProxyServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

    def __init__(
        self,
        target_port: int,
        response_delay_ms: int,
        peer_credentials: PeerCredentials | None,
        target_node_id: int | None,
    ) -> None:
        if (peer_credentials is None) != (target_node_id is None):
            raise ValueError("TLS credentials and target node ID must be configured together")

        self.target_port = target_port
        self.response_delay_ms = response_delay_ms
        self.response_delay_seconds = response_delay_ms / 1_000
        self.stats_lock = threading.Lock()
        self.connection_count = 0
        self.active_connections = 0
        self.max_active_connections = 0
        self.request_count = 0
        self.response_count = 0
        self.delayed_response_count = 0
        self.peer_credentials = peer_credentials
        self.target_node_id = target_node_id
        self.server_tls_context = None
        self.client_tls_contexts: dict[int, ssl.SSLContext] = {}
        if peer_credentials is not None and target_node_id is not None:
            node = peer_credentials.node(target_node_id)
            server_tls_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_tls_context.minimum_version = ssl.TLSVersion.TLSv1_3
            server_tls_context.maximum_version = ssl.TLSVersion.TLSv1_3
            server_tls_context.verify_mode = ssl.CERT_REQUIRED
            server_tls_context.load_verify_locations(cafile=str(node.trust_bundle))
            server_tls_context.load_cert_chain(
                certfile=str(node.certificate_chain),
                keyfile=str(node.private_key),
            )
            self.server_tls_context = server_tls_context
        super().__init__(("127.0.0.1", 0), _PeerDelayProxyHandler)

    def client_tls_context(self, source_node_id: int) -> ssl.SSLContext:
        context = self.client_tls_contexts.get(source_node_id)
        if context is not None:
            return context
        assert self.peer_credentials is not None
        node = self.peer_credentials.node(source_node_id)
        context = ssl.create_default_context(
            ssl.Purpose.SERVER_AUTH,
            cafile=str(node.trust_bundle),
        )
        context.minimum_version = ssl.TLSVersion.TLSv1_3
        context.maximum_version = ssl.TLSVersion.TLSv1_3
        context.load_cert_chain(
            certfile=str(node.certificate_chain),
            keyfile=str(node.private_key),
        )
        self.client_tls_contexts[source_node_id] = context
        return context


class _PeerDelayProxyHandler(socketserver.BaseRequestHandler):
    def handle(self) -> None:
        server = self.server
        assert isinstance(server, _PeerDelayProxyServer)
        with server.stats_lock:
            server.connection_count += 1
            server.active_connections += 1
            server.max_active_connections = max(
                server.max_active_connections, server.active_connections
            )
        try:
            self.request.settimeout(DEFAULT_TIMEOUT_SECONDS)
            if server.server_tls_context is None:
                self._handle_plaintext(server)
            else:
                self._handle_tls(server)
        except (BenchmarkError, ConnectionError, OSError, ssl.SSLError):
            return
        finally:
            with server.stats_lock:
                server.active_connections -= 1

    def _handle_plaintext(self, server: _PeerDelayProxyServer) -> None:
        with socket.create_connection(
            ("127.0.0.1", server.target_port), timeout=DEFAULT_TIMEOUT_SECONDS
        ) as target:
            self._proxy_frames(server, self.request, target)

    def _handle_tls(self, server: _PeerDelayProxyServer) -> None:
        assert server.server_tls_context is not None
        assert server.peer_credentials is not None
        assert server.target_node_id is not None
        with server.server_tls_context.wrap_socket(
            self.request,
            server_side=True,
        ) as client:
            peer_certificate = client.getpeercert()
            dns_identities = [
                identity
                for name_type, identity in peer_certificate.get("subjectAltName", ())
                if name_type == "DNS"
            ]
            if len(dns_identities) != 1:
                return
            source_node_id = server.peer_credentials.node_id_for_identity(dns_identities[0])
            if source_node_id is None or source_node_id == server.target_node_id:
                return
            with socket.create_connection(
                ("127.0.0.1", server.target_port), timeout=DEFAULT_TIMEOUT_SECONDS
            ) as target_socket:
                with server.client_tls_context(source_node_id).wrap_socket(
                    target_socket,
                    server_hostname=peer_identity(
                        server.target_node_id,
                        server.peer_credentials.cluster_name,
                    ),
                ) as target:
                    self._proxy_frames(server, client, target)

    def _proxy_frames(
        self,
        server: _PeerDelayProxyServer,
        client: socket.socket,
        target: socket.socket,
    ) -> None:
        while True:
            frame = _read_proxy_frame(client)
            if frame is None:
                return
            target.sendall(frame)
            with server.stats_lock:
                server.request_count += 1
            response = _read_proxy_frame(target)
            if response is None:
                return
            if server.response_delay_seconds and _is_forward_response(response):
                time.sleep(server.response_delay_seconds)
                with server.stats_lock:
                    server.delayed_response_count += 1
            client.sendall(response)
            with server.stats_lock:
                server.response_count += 1


class PeerResponseDelayProxy:
    """Delay framed peer responses while preserving the real TCP peer path."""

    def __init__(
        self,
        target_port: int,
        response_delay_ms: int,
        peer_credentials: PeerCredentials | None = None,
        target_node_id: int | None = None,
    ) -> None:
        self.server = _PeerDelayProxyServer(
            target_port,
            response_delay_ms,
            peer_credentials,
            target_node_id,
        )
        self.started = False
        self.thread = threading.Thread(
            target=self.server.serve_forever,
            name=f"runnel-peer-delay-{self.server.server_address[1]}",
            daemon=True,
        )

    @property
    def port(self) -> int:
        return int(self.server.server_address[1])

    def start(self) -> None:
        if self.started:
            return
        self.thread.start()
        self.started = True

    def close(self) -> None:
        if self.started:
            self.server.shutdown()
        self.server.server_close()
        if self.started:
            self.thread.join(timeout=5)

    def summary(self) -> dict[str, Any]:
        with self.server.stats_lock:
            return {
                "target_port": self.server.target_port,
                "listen_port": self.port,
                "response_delay_ms": self.server.response_delay_ms,
                "connections": self.server.connection_count,
                "max_active_connections": self.server.max_active_connections,
                "requests": self.server.request_count,
                "responses": self.server.response_count,
                "delayed_responses": self.server.delayed_response_count,
            }
