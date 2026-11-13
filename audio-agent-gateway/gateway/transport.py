"""Outbound limits from upstream core/websocket-send.mjs."""

import os
import random
from functools import wraps


class _ClosingSocket:
    def __init__(self, sock):
        self.sock = sock

    def __getattr__(self, name):
        return getattr(self.sock, name)

    def shutdown(self, how):
        try:
            self.sock.shutdown(how)
        except ConnectionResetError:
            pass


def install_windows_cleanup():
    if os.name != "nt":
        return
    from asyncio.proactor_events import _ProactorBasePipeTransport

    original = _ProactorBasePipeTransport._call_connection_lost
    if getattr(original, "gateway_reset_safe", False):
        return

    @wraps(original)
    def connection_lost(transport, exc):
        # CPython's shutdown() can raise WinError 10054 before socket.close/server._detach.
        sock = transport._sock
        if sock is None or transport._called_connection_lost or not hasattr(sock, "shutdown"):
            return original(transport, exc)
        closing = _ClosingSocket(sock)
        transport._sock = closing
        try:
            return original(transport, exc)
        finally:
            if transport._sock is closing:
                transport._sock = sock

    connection_lost.gateway_reset_safe = True
    _ProactorBasePipeTransport._call_connection_lost = connection_lost


class ReconnectBackoff:
    def __init__(self):
        self.attempt = 0

    def next(self, connected_seconds=0):
        if connected_seconds >= 10:
            self.attempt = 0
        milliseconds = min(10000, 500 * 2 ** min(self.attempt, 8))
        self.attempt += 1
        return int(milliseconds * (0.8 + 0.4 * random.random()) + 0.5) / 1000


def within_budget(size, buffered, *, audio=False):
    limit = 512 * 1024 if audio else max(1024 * 1024, size)
    return size <= 20 * 1024 * 1024 and buffered + size <= limit
