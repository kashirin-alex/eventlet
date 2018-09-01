import errno
import sys
from eventlet import patcher
from eventlet.support import get_errno, clear_sys_exc_info
from eventlet.hubs.hub import BaseHub, noop_r, noop_w

select = patcher.original('select')
ev_sleep = patcher.original('time').sleep

try:
    BAD_SOCK = set((errno.EBADF, errno.WSAENOTSOCK))
except AttributeError:
    BAD_SOCK = set((errno.EBADF,))


class Hub(BaseHub):

    def _remove_bad_fds(self):
        """ Iterate through fds, removing the ones that are bad per the
        operating system.
        """
        for fd in list(self.listeners_read) + list(self.listeners_write):
            try:
                select.select([fd], [], [], 0)
            except select.error as e:
                if get_errno(e) in BAD_SOCK:
                    self.remove_descriptor(fd)

    def wait(self, seconds=0):
        if not self.listeners_read and not self.listeners_write:
            ev_sleep(seconds)
            return

        try:
            rs, ws, es = select.select(self.listeners_read.keys(), self.listeners_write.keys(),
                                       list(self.listeners_read) + list(self.listeners_write), seconds)
        except select.error as e:
            if get_errno(e) == errno.EINTR:
                return
            elif get_errno(e) in BAD_SOCK:
                self._remove_bad_fds()
                return
            else:
                raise

        for fileno in rs:
            try:
                self.listeners_read.get(fileno, noop_r).cb(fileno)
            except self.SYSTEM_EXCEPTIONS:
                raise
            except:
                self.squelch_exception(fileno, sys.exc_info())
                clear_sys_exc_info()

        for fileno in ws:
            try:
                self.listeners_write.get(fileno, noop_w).cb(fileno)
            except self.SYSTEM_EXCEPTIONS:
                raise
            except:
                self.squelch_exception(fileno, sys.exc_info())
                clear_sys_exc_info()

        for fileno in es:
            try:
                self.listeners_read.get(fileno, noop_r).cb(fileno)
                self.listeners_write.get(fileno, noop_w).cb(fileno)
            except self.SYSTEM_EXCEPTIONS:
                raise
            except:
                self.squelch_exception(fileno, sys.exc_info())
                clear_sys_exc_info()
