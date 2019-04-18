import errno
import sys
import socket
import ssl as _ssl

import eventlet
from eventlet.hubs import trampoline, notify_opened, IOClosed, active_hub
from eventlet.support import get_errno
import six

__all__ = [
    'UltraGreenSocket', '_GLOBAL_DEFAULT_TIMEOUT',
    'SOCKET_BLOCKING', 'SOCKET_CLOSED', 'CONNECT_ERR', 'CONNECT_SUCCESS',
    'shutdown_safe', 'SSL',
    'socket_timeout',
]
try:
    from OpenSSL import SSL
except ImportError:
    # pyOpenSSL not installed, define exceptions anyway for convenience
    class SSL(object):
        class WantWriteError(Exception):
            pass

        class WantReadError(Exception):
            pass

        class ZeroReturnError(Exception):
            pass

        class SysCallError(Exception):
            pass
SSLError = _ssl.SSLError
#

BUFFER_SIZE = 4096
CONNECT_ERR = set((errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK))
CONNECT_SUCCESS = set((0, errno.EISCONN))
if sys.platform[:3] == "win":
    CONNECT_ERR.add(errno.WSAEINVAL)   # Bug 67


if sys.platform[:3] == "win":
    # winsock sometimes throws ENOTCONN
    SOCKET_BLOCKING = set((errno.EAGAIN, errno.EWOULDBLOCK,))
    SOCKET_CLOSED = set((errno.ECONNRESET, errno.ENOTCONN, errno.ESHUTDOWN))
else:
    # oddly, on linux/darwin, an unconnected socket is expected to block,
    # so we treat ENOTCONN the same as EWOULDBLOCK
    SOCKET_BLOCKING = set((errno.EAGAIN, errno.EWOULDBLOCK, errno.ENOTCONN))
    SOCKET_CLOSED = set((errno.ECONNRESET, errno.ESHUTDOWN, errno.EPIPE))

if six.PY2:
    _python2_fileobject = socket._fileobject

_original_socket = eventlet.patcher.original('socket').socket

_GLOBAL_DEFAULT_TIMEOUT = getattr(socket, '_GLOBAL_DEFAULT_TIMEOUT', object())
socket_timeout = eventlet.timeout.wrap_is_timeout(socket.timeout)
#


def socket_connect(descriptor, address):
    """
    Attempts to connect to the address, returns the descriptor if it succeeds,
    returns None if it needs to trampoline, and raises any exceptions.
    """
    err = descriptor.connect_ex(address)
    if err in CONNECT_ERR:
        return None
    if err not in CONNECT_SUCCESS:
        raise socket.error(err, errno.errorcode[err])
    return descriptor


def socket_checkerr(descriptor):
    err = descriptor.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if err not in CONNECT_SUCCESS:
        raise socket.error(err, errno.errorcode[err])


def socket_accept(descriptor):
    """
    Attempts to accept() on the descriptor, returns a client,address tuple
    if it succeeds; returns None if it needs to trampoline, and raises
    any exceptions.
    """
    try:
        return descriptor.accept()
    except socket.error as e:
        if get_errno(e) == errno.EWOULDBLOCK:
            return None
        raise


def shutdown_safe(sock):
    """Shuts down the socket. This is a convenience method for
    code that wants to gracefully handle regular sockets, SSL.Connection
    sockets from PyOpenSSL and ssl.SSLSocket objects from Python 2.7 interchangeably.
    Both types of ssl socket require a shutdown() before close,
    but they have different arity on their shutdown method.

    Regular sockets don't need a shutdown before close, but it doesn't hurt.
    """
    try:
        try:
            # socket, ssl.SSLSocket
            return sock.shutdown(socket.SHUT_RDWR)
        except TypeError:
            # SSL.Connection
            return sock.shutdown()
    except socket.error as e:
        # we don't care if the socket is already closed;
        # this will often be the case in an http server context
        if get_errno(e) not in (errno.ENOTCONN, errno.EBADF, errno.ENOTSOCK):
            raise


def _operation_on_closed_file(*args, **kwargs):
    raise ValueError("I/O operation on closed file")


#
class UltraGreenSocket(object):

    """
    UltraGreen version of socket.socket + ssl.SSLSocket + SSL.SSLConnection classes,
    performance optimal intend
    """

    __slots__ = ['fd', '_timeout',
                 'fileno', 'getsockname',
                 'getsockopt', 'setsockopt',
                 'bind', 'listen', 'shutdown',
                 'is_ssl', '_closed']

    def __init__(self, family=socket.AF_INET, *args, **kwargs):
        self._closed = True
        self._timeout = kwargs.pop('timeout', None)

        fd = kwargs.pop('fd', None)
        if fd is None:
            fd = _original_socket(family, *args, **kwargs)
            # Notify the hub that this is a newly-opened socket.
            notify_opened(fd.fileno())

        self._setup(fd, self._timeout)
        #

    def _setup(self, fd, timeout):

        fd.setblocking(0)
        fd.settimeout(0.0)
        self.settimeout(timeout)

        # Copy some attributes from underlying real socket.

        self.bind = fd.bind
        self.fileno = fd.fileno
        self.getsockname = fd.getsockname
        self.getsockopt = fd.getsockopt
        self.listen = fd.listen
        self.setsockopt = fd.setsockopt
        self.shutdown = fd.shutdown

        self.fd = fd
        self._closed = False
        self.is_ssl = hasattr(fd, 'ssl_version')
        #

    def settimeout(self, timeout):
        self._timeout = timeout
        #

    def gettimeout(self):
        return self._timeout
        #

    @property
    def _sock(self):
        return self

    def dup(self, *args, **kw):
        if self.is_ssl:
            raise NotImplementedError("Can't dup an ssl object")
        return UltraGreenSocket(fd=self.fd.dup(*args, **kw))
        #

    if six.PY3:
        def _get_io_refs(self):
            return self.fd._io_refs

        def _set_io_refs(self, value):
            self.fd._io_refs = value

        _io_refs = property(_get_io_refs, _set_io_refs)

    def __getattr__(self, name):
        if self.fd is None:
            raise AttributeError(name)
        return getattr(self.fd, name)

    if "__pypy__" in sys.builtin_module_names:
        def _reuse(self):
            getattr(self.fd, '_sock', self.fd)._reuse()

        def _drop(self):
            getattr(self.fd, '_sock', self.fd)._drop()
    #

    def _trampoline(self, **kw):
        """ We need to trampoline via the event hub.
            We catch any signal back from the hub indicating that the operation we
            were waiting on was associated with a filehandle that's since been
            invalidated.
        """
        if self._closed:
            # If we did any logging, alerting to a second trampoline attempt on a closed
            # socket here would be useful.
            raise IOClosed()
        try:
            kw['mark_as_closed'] = self._mark_as_closed
            return trampoline(self.fd, **kw)
        except IOClosed:
            # This socket's been obsoleted. De-fang it.
            self._mark_as_closed()
            raise
        #

    def _mark_as_closed(self):
        """ Mark this socket as being closed """
        self._closed = True
        #

    def connect(self, address):
        fd = self.fd
        if self._timeout is None:
            while not socket_connect(fd, address):
                try:
                    self._trampoline(write=True)
                except IOClosed:
                    raise socket.error(errno.EBADFD)
                socket_checkerr(fd)
            return

        clock = active_hub.inst.clock
        end = clock() + self._timeout
        while not socket_connect(fd, address):
            if clock() >= end:
                raise socket_timeout('timed out')
            try:
                self._trampoline(write=True, timeout=end - clock(), timeout_exc=socket_timeout('timed out'))
            except IOClosed:
                raise socket.error(errno.EBADFD)
            socket_checkerr(fd)
        #

    def connect_ex(self, address):
        try:
            self.connect(address)
        except Exception as e:
            return get_errno(e)
        #

    def accept(self):
        fd = self.fd
        _timeout_exc = socket_timeout('timed out')
        while True:
            res = socket_accept(fd)
            if res is not None:
                client, addr = res
                notify_opened(client.fileno())

                if not self.is_ssl:
                    return UltraGreenSocket(fd=client), addr

                new_ssl = _ssl.SSLSocket(
                    client,
                    keyfile=fd.keyfile,
                    certfile=fd.certfile,
                    server_side=True,
                    cert_reqs=fd.cert_reqs,
                    ssl_version=fd.ssl_version,
                    ca_certs=fd.ca_certs,
                    do_handshake_on_connect=False,
                    suppress_ragged_eofs=fd.suppress_ragged_eofs)
                return UltraGreenSocket(fd=new_ssl), addr

            self._trampoline(read=True, timeout=self._timeout, timeout_exc=_timeout_exc)
        #

    if six.PY3:
        def makefile(self, *args, **kwargs):
            if self.is_ssl:
                raise NotImplementedError("Makefile not supported on SSL sockets")
            return _original_socket.makefile(self, *args, **kwargs)
    else:
        def makefile(self, *args, **kwargs):
            if self.is_ssl:
                raise NotImplementedError("Makefile not supported on SSL sockets")
            dupped = self.dup()
            res = _python2_fileobject(dupped, *args, **kwargs)
            if hasattr(dupped, "_drop"):
                dupped._drop()
                # Making the close function of dupped None so that when garbage collector
                # kicks in and tries to call del, which will ultimately call close, _drop
                # doesn't get called on dupped twice as it has been already explicitly called in
                # previous line
                dupped.close = None
            return res

    def _recv_loop(self, recv_meth, empty_val, *args):
        _timeout_exc = socket_timeout('timed out')
        while True:
            try:
                # recv: bufsize=0?
                # recv_into: buffer is empty?
                # This is needed because behind the scenes we use sockets in
                # nonblocking mode and builtin recv* methods. Attempting to read
                # 0 bytes from a nonblocking socket using a builtin recv* method
                # does not raise a timeout exception. Since we're simulating
                # a blocking socket here we need to produce a timeout exception
                # if needed, hence the call to trampoline.
                if self.is_ssl and not args[0]:
                    self._trampoline(read=True, timeout=self._timeout, timeout_exc=_timeout_exc)
                return recv_meth(*args)

            except SSLError as e:
                self._trampoline_ssl(e)
                continue

            except socket.error as e:
                if get_errno(e) in SOCKET_BLOCKING:
                    pass
                elif get_errno(e) in SOCKET_CLOSED:
                    return empty_val
                else:
                    raise

            if self.is_ssl:
                continue

            try:
                self._trampoline(read=True, timeout=self._timeout, timeout_exc=_timeout_exc)
            except IOClosed as e:
                # Perhaps we should return 'empty_val' instead?
                raise EOFError()
        #

    def recv(self, bufsize, flags=0):
        return self._recv_loop(self.fd.recv, b'', bufsize, flags)

    def recvfrom(self, bufsize, flags=0):
        return self._recv_loop(self.fd.recvfrom, b'', bufsize, flags)

    def recv_into(self, buff, nbytes=0, flags=0):
        if self.is_ssl:
            if buff and nbytes is None:
                nbytes = len(buff)
            elif nbytes is None:
                nbytes = 1024
        return self._recv_loop(self.fd.recv_into, 0, buff, nbytes, flags)

    def recvfrom_into(self, buff, nbytes=0, flags=0):
        return self._recv_loop(self.fd.recvfrom_into, 0, buff, nbytes, flags)

    def _trampoline_ssl(self, e):
        if get_errno(e) == _ssl.SSL_ERROR_WANT_READ:
            self._trampoline(read=True, timeout=self._timeout, timeout_exc=SSLError('timed out'))
            return
        if get_errno(e) == _ssl.SSL_ERROR_WANT_WRITE:
            self._trampoline(write=True, timeout=self._timeout, timeout_exc=SSLError('timed out'))
            return
        raise e
        #

    def _send_loop(self, send_method, data, *args):

        while True:
            try:
                return send_method(data, *args)

            except SSLError as e:
                self._trampoline_ssl(e)
                continue

            except socket.error as e:
                eno = get_errno(e)
                if eno in SOCKET_CLOSED or eno not in SOCKET_BLOCKING:
                    raise socket.error(eno)

            if self.is_ssl:
                continue

            try:
                self._trampoline(write=True, timeout=self._timeout, timeout_exc=socket_timeout('timed out'))
            except IOClosed:
                raise socket.error(errno.ECONNRESET, 'Connection closed by another thread')
        #

    def send(self, data, flags=0):
        return self._send_loop(self.fd.send, data, flags)

    def sendto(self, data, *args):
        return self._send_loop(self.fd.sendto, data, *args)

    def sendall(self, data, flags=0):
        while data:
            offset = self._send_loop(self.fd.send, data, flags)
            if offset > 0:
                data = data[offset:]
        #

    def wrap_socket(self, ctx, **kw):
        self._setup(SSL.Connection(ctx, self.fd) if kw.pop('accept_state', None)
                    else ctx.wrap_socket(self.fd, **kw),
                    self._timeout)
        #

    def unwrap(self):
        while True:
            try:
                self._setup(self.fd.unwrap())
                return self.fd
            except SSLError as e:
                self._trampoline_ssl(e)
        #

    def do_handshake(self):
        """Perform a TLS/SSL handshake."""
        while True:
            try:
                return self.fd.do_handshake()
            except SSLError as e:
                self._trampoline_ssl(e)
        #

    def close(self):
        if self.fd is None:
            return
        self.fd.close()
        self._mark_as_closed()
        #

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    def __del__(self):
        if self.fd is None:
            return
        self.fd.close()
        self._mark_as_closed()
        #
#


# from eventlet.greenio.ultra import UltraGreenSocket

# r = UltraGreenSocket(af, socket.SOCK_STREAM)
#    or
# r =  UltraGreenSocket(fd=r)
#    or SSL socket (r=ctx.wrap_socket(r, **kw))
# r =  UltraGreenSocket(fd=r)
#    or
# r = UltraGreenSocket(af, socket.SOCK_STREAM)
# r.conn ... ()
# r.wrap_socket(ctx)
# r.unwrap()
