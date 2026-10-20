"""Run pytest with a kernel-enforced, child-inherited outbound network deny."""
import ctypes
import errno
import socket
import sys
lib = ctypes.CDLL('libseccomp.so.2', use_errno=True)
lib.seccomp_init.argtypes = [ctypes.c_uint32]
lib.seccomp_init.restype = ctypes.c_void_p
lib.seccomp_syscall_resolve_name.argtypes = [ctypes.c_char_p]
lib.seccomp_syscall_resolve_name.restype = ctypes.c_int
lib.seccomp_rule_add.argtypes = [ctypes.c_void_p, ctypes.c_uint32, ctypes.c_int, ctypes.c_uint]
lib.seccomp_load.argtypes = [ctypes.c_void_p]
lib.seccomp_release.argtypes = [ctypes.c_void_p]
ctx = lib.seccomp_init(0x7fff0000)
if not ctx:
    raise RuntimeError('seccomp_init failed; refusing to run tests')
for name in (b'connect', b'sendto', b'sendmsg', b'sendmmsg'):
    nr = lib.seccomp_syscall_resolve_name(name)
    if nr < 0 or lib.seccomp_rule_add(ctx, 0x00050000 | errno.EPERM, nr, 0) != 0:
        raise RuntimeError('cannot install network denial; refusing to run tests')
if lib.seccomp_load(ctx) != 0:
    raise RuntimeError('cannot load network denial; refusing to run tests')
lib.seccomp_release(ctx)
with socket.socket() as sock:
    try:
        sock.connect(('127.0.0.1', 9))
        raise AssertionError('network filter did not load')
    except OSError as exc:
        if exc.errno != errno.EPERM:
            raise RuntimeError('network denial verification failed') from exc
print('NETWORK_DISABLED: kernel denies connect/sendto/sendmsg/sendmmsg, inherited by all children', flush=True)
if '--verify-only' not in sys.argv:
    import pytest
    raise SystemExit(pytest.main(sys.argv[1:]))
