import errno
import sys

import eventlet
from eventlet.hubs import trampoline, notify_opened, IOClosed, active_hub
from eventlet.support import get_errno
import six

from eventlet.green.OpenSSL.SSL import orig_SSL as SSL
ssl = eventlet.patcher.original('ssl')
socket = eventlet.patcher.original('socket')

__all__ = [
    'UltraGreenSocket',
    'CONNECT_SUCCESS', 'SOCKET_BLOCKING', 'SOCKET_CLOSED', 'CONNECT_ERR',
]
#

BUFFER_SIZE = 4096
CONNECT_SUCCESS = set((0, errno.EISCONN))
SOCKET_BLOCKING = set((errno.EAGAIN, errno.EWOULDBLOCK))
SOCKET_CLOSED = set((errno.ECONNRESET, errno.ESHUTDOWN))
CONNECT_ERR = set((errno.EINPROGRESS, errno.EALREADY, errno.EWOULDBLOCK))

if sys.platform[:3] == "win":
    CONNECT_ERR.add(errno.WSAEINVAL)   # Bug 67
    SOCKET_CLOSED.add(errno.ENOTCONN)  # winsock sometimes throws ENOTCONN
else:
    # oddly, on linux/darwin, an unconnected socket is expected to block,
    # so we treat ENOTCONN the same as EWOULDBLOCK
    SOCKET_BLOCKING.add(errno.ENOTCONN)
    SOCKET_CLOSED.add(errno.EPIPE)

ex_want_read = (ssl.SSLWantReadError, SSL.WantReadError)
ex_want_write = (ssl.SSLWantWriteError, SSL.WantWriteError)
ex_return_zero = (ssl.SSLZeroReturnError, SSL.ZeroReturnError)

timeout_exc = eventlet.timeout.wrap_is_timeout(socket.timeout)(errno.ETIMEDOUT, 'timed out')
# timeout_ssl_exc = ssl.SSLError(errno.ETIMEDOUT, 'timed out')
ex_timeout = (type(timeout_exc), )
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
    #


def socket_checkerr(descriptor):
    err = descriptor.getsockopt(socket.SOL_SOCKET, socket.SO_ERROR)
    if err not in CONNECT_SUCCESS:
        raise socket.error(err, errno.errorcode[err])
    #


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
    #


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
            fd = socket.socket(family, *args, **kwargs)
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
        # self._timeout_exc = timeout_ssl_exc if self.is_ssl else timeout_exc
        #

    @property
    def _sock(self):
        return self

    def settimeout(self, timeout):
        self._timeout = timeout
        #

    def gettimeout(self):
        return self._timeout
        #

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
        # todo, listener == self
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
                raise timeout_exc
            try:
                self._trampoline(write=True, timeout=end - clock(), timeout_exc=timeout_exc)
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
        while True:
            res = socket_accept(fd)
            if res is not None:
                client, addr = res
                notify_opened(client.fileno())

                if not self.is_ssl:
                    return UltraGreenSocket(fd=client), addr

                new_ssl = ssl.SSLSocket(
                    client,
                    keyfile=fd.keyfile,
                    certfile=fd.certfile,
                    server_side=True,
                    cert_reqs=fd.cert_reqs,
                    ssl_version=fd.ssl_version,
                    ca_certs=fd.ca_certs,
                    do_handshake_on_connect=False,
                    suppress_ragged_eofs=fd.suppress_ragged_eofs)
                fd = UltraGreenSocket(fd=new_ssl)
                fd.do_handshake()
                return fd, addr

            self._trampoline(read=True, timeout=self._timeout, timeout_exc=timeout_exc)
        #

    if six.PY3:
        def makefile(self, *args, **kwargs):
            if self.is_ssl:
                raise NotImplementedError("Makefile not supported on SSL sockets")
            return socket.socket.makefile(self, *args, **kwargs)
    else:
        def makefile(self, *args, **kwargs):
            if self.is_ssl:
                raise NotImplementedError("Makefile not supported on SSL sockets")
            dupped = self.dup()
            res = socket._fileobject(dupped, *args, **kwargs)
            if hasattr(dupped, "_drop"):
                dupped._drop()
                # Making the close function of dupped None so that when garbage collector
                # kicks in and tries to call del, which will ultimately call close, _drop
                # doesn't get called on dupped twice as it has been already explicitly called in
                # previous line
                dupped.close = None
            return res

    def recv(self, bufsize, flags=0):
        return self._recv_loop(self.fd.recv, b'', bufsize, flags)
    read = recv

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

    def _trampoline_on_possible(self, e, read=False, write=False):
        type_e = type(e)

        if type_e in ex_timeout:
            raise e
        elif type_e in ex_want_read:
            read = True
            write = False
        elif type_e in ex_want_write:
            read = False
            write = True
        elif type_e in ex_return_zero:
            if not read:
                raise e
            return True

        elif socket.error is type_e:
            eno = get_errno(e)
            if eno in SOCKET_BLOCKING:
                pass
            elif eno in SOCKET_CLOSED:
                if not read:
                    raise e
                return True
            else:
                raise e
        else:
            raise e

        if read or write:
            try:
                self._trampoline(read=read, write=write, timeout=self._timeout, timeout_exc=timeout_exc)
                return
            except IOClosed:
                raise socket.error(errno.ECONNRESET, 'Connection closed by another thread')
            except Exception as e:
                raise e

        raise e
        #

    def _recv_loop(self, recv_meth, empty_val, *args):
        while True:
            try:
                if not self.is_ssl and not args[0]:  # no timeout on Zero bytes to read
                    self._trampoline(read=True, timeout=self._timeout, timeout_exc=timeout_exc)

                return recv_meth(*args)
            except Exception as e:
                if self._trampoline_on_possible(e, read=True):
                    return empty_val
                    #

    def _send_loop(self, send_method, data, *args):
        while True:
            try:
                return send_method(data, *args)
            except Exception as e:
                self._trampoline_on_possible(e, write=True)
        #

    def send(self, data, flags=0):
        return self._send_loop(self.fd.send, data, flags)
    write = send

    def sendto(self, data, *args):
        return self._send_loop(self.fd.sendto, data, *args)

    def sendall(self, data, flags=0):
        while data:
            offset = self._send_loop(self.fd.send, data, flags)
            if offset > 0:
                data = data[offset:]
        #

    def ssl_wrap(self, ctx, **kw):
        if isinstance(ctx, SSL.Context):
            self._setup(SSL.Connection(ctx, self.fd), self._timeout)

        elif isinstance(ctx, ssl.SSLContext):
            kw['do_handshake_on_connect'] = False
            self._setup(ctx.wrap_socket(self.fd, **kw), self._timeout)
            self.do_handshake()

        # else Additional ssl-context types

        #
    wrap_socket = ssl_wrap

    def ssl_unwrap(self):
        while True:
            try:
                self._setup(self.fd.unwrap(), self._timeout)
                return self.fd
            except Exception as e:
                self._trampoline_on_possible(e)
        #
    unwrap = ssl_unwrap

    def do_handshake(self):
        """Perform a TLS/SSL handshake."""
        while True:
            try:
                return self.fd.do_handshake()
            except Exception as e:
                self._trampoline_on_possible(e)
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
#    or, init with SSL socket
# r =  UltraGreenSocket(fd=ctx.wrap_socket(r, **kw))
#    or
# with UltraGreenSocket(af, socket.SOCK_STREAM) as r:
#   r.conn ... ()
#   r.ssl_wrap(ctx)
#   r.ssl_unwrap()
