#!/usr/bin/env python3
"""Bounded fault-injection primitives for the clustered benchmark."""

from __future__ import annotations

import json
import socket
import socketserver
import threading
import time
from typing import Any

from common import BenchmarkError, DEFAULT_TIMEOUT_SECONDS


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

    def __init__(self, target_port: int, response_delay_ms: int) -> None:
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
        super().__init__(("127.0.0.1", 0), _PeerDelayProxyHandler)


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
            with socket.create_connection(
                ("127.0.0.1", server.target_port), timeout=DEFAULT_TIMEOUT_SECONDS
            ) as target:
                self.request.settimeout(DEFAULT_TIMEOUT_SECONDS)
                while True:
                    frame = _read_proxy_frame(self.request)
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
                    self.request.sendall(response)
                    with server.stats_lock:
                        server.response_count += 1
        except (BenchmarkError, ConnectionError, OSError):
            return
        finally:
            with server.stats_lock:
                server.active_connections -= 1


class PeerResponseDelayProxy:
    """Delay framed peer responses while preserving the real TCP peer path."""

    def __init__(self, target_port: int, response_delay_ms: int) -> None:
        self.server = _PeerDelayProxyServer(target_port, response_delay_ms)
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
