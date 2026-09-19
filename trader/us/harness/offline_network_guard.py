"""Pytest plugin: forbid external connections, allowing local test PostgreSQL.

Load with -p trader.us.harness.offline_network_guard. This is test-only and
must be installed before collection because some legacy tests import clients.
"""
import ipaddress
import socket

_connect = socket.socket.connect
_connect_ex = socket.socket.connect_ex


def _check(address):
    if not isinstance(address, tuple):  # Unix-domain socket
        return
    host = str(address[0])
    # Restrict the port too: this environment may expose an external HTTP
    # proxy on a loopback address. Only the local PostgreSQL service is allowed.
    if len(address) < 2 or address[1] != 5432:
        raise RuntimeError("OFFLINE_TEST_EXTERNAL_NETWORK_BLOCKED")
    if host == "localhost":
        return
    try:
        if ipaddress.ip_address(host).is_loopback:
            return
    except ValueError:
        pass
    raise RuntimeError("OFFLINE_TEST_EXTERNAL_NETWORK_BLOCKED")


def _guarded_connect(self, address):
    _check(address)
    return _connect(self, address)


def _guarded_connect_ex(self, address):
    _check(address)
    return _connect_ex(self, address)


socket.socket.connect = _guarded_connect
socket.socket.connect_ex = _guarded_connect_ex
