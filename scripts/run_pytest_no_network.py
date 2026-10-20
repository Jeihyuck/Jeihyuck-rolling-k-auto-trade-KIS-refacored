"""Linux pytest runner with inherited kernel IP-network denial.

AF_UNIX socketpairs remain available for asyncio's internal wakeup pipe.
No IPv4/IPv6/raw socket can be created and inherited sockets cannot connect.
Failure to load or verify the filter aborts before importing pytest.
"""
import ctypes
import errno
import socket
import sys


class ArgCompare(ctypes.Structure):
    _fields_ = [('arg', ctypes.c_uint), ('op', ctypes.c_int),
                ('datum_a', ctypes.c_uint64), ('datum_b', ctypes.c_uint64)]


lib = ctypes.CDLL('libseccomp.so.2', use_errno=True)
lib.seccomp_init.argtypes = [ctypes.c_uint32]
lib.seccomp_init.restype = ctypes.c_void_p
lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
lib.seccomp_syscall_resolve_name.restype = ctypes.c_int
lib.seccomp_rule_add_array.argtypes = [ctypes.c_void_p, ctypes.c_uint32,
                                     ctypes.c_int, ctypes.c_uint,
                                     ctypes.POINTER(ArgCompare)]
lib.seccomp_load.argtypes = [ctypes.c_void_p]
lib.seccomp_release.argtypes = [ctypes.c_void_p]
ctx = lib.seccomp_init(0x7fff0000)  # SCMP_ACT_ALLOW
if not ctx:
    raise RuntimeError('seccomp_init failed; refusing to run tests')
deny = 0x00050000 | errno.EPERM
for name, predicates in (
    (b'connect', []),
    (b'socket', [ArgCompare(0, 1, socket.AF_UNIX, 0)]),  # SCMP_CMP_NE
    (b'socketpair', [ArgCompare(0, 1, socket.AF_UNIX, 0)]),
):
    nr = lib.seccomp_syscall_resolve_name(name)
    args = (ArgCompare * len(predicates))(*predicates) if predicates else None
    if nr < 0 or lib.seccomp_rule_add_array(ctx, deny, nr, len(predicates), args) != 0:
        raise RuntimeError('cannot install IP-network denial; refusing to run tests')
if lib.seccomp_load(ctx) != 0:
    raise RuntimeError('cannot load IP-network denial; refusing to run tests')
lib.seccomp_release(ctx)
for family in (socket.AF_INET, socket.AF_INET6):
    try:
        sock = socket.socket(family)
    except OSError as exc:
        if exc.errno != errno.EPERM:
            raise RuntimeError('IP-network denial verification failed') from exc
    else:
        sock.close()
        raise RuntimeError('IP-network filter did not load')
# asyncio uses AF_UNIX socketpairs for its local wakeup pipe.
a, b = socket.socketpair()
a.send(b'check')
if b.recv(5) != b'check':
    raise RuntimeError('local socketpair verification failed')
a.close()
b.close()
print('NETWORK_DISABLED: kernel denies non-UNIX sockets and connect; inherited by all children', flush=True)
if '--verify-only' not in sys.argv:
    import pytest
    raise SystemExit(pytest.main(sys.argv[1:]))
