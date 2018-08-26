import errno
import sys
from eventlet import patcher
from eventlet.support import get_errno, clear_sys_exc_info
from eventlet.hubs.hub import BaseHub

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
        for fd in list(self.readers) + list(self.writers):
            try:
                select.select([fd], [], [], 0)
            except select.error as e:
                if get_errno(e) in BAD_SOCK:
                    self.remove_descriptor(fd)

    def wait(self, seconds=None):
        if not self.readers and not self.writers:
            if seconds:
                ev_sleep(seconds)
            return

        try:
            r, w, er = select.select(self.readers.keys(), self.writers.keys(),
                                     list(self.readers) + list(self.writers), seconds)
        except select.error as e:
            if get_errno(e) == errno.EINTR:
                return
            elif get_errno(e) in BAD_SOCK:
                self._remove_bad_fds()
                return
            else:
                raise

        for fileno in er:
            l = self.readers.get(fileno)
            if l:
                l.cb(fileno)
            l = self.writers.get(fileno)
            if l:
                l.cb(fileno)

        for listeners, events in ((self.readers, r), (self.writers, w)):
            for fileno in events:
                try:
                    l = listeners.get(fileno)
                    if l:
                        l.cb(fileno)
                except self.SYSTEM_EXCEPTIONS:
                    raise
                except:
                    self.squelch_exception(fileno, sys.exc_info())
                    clear_sys_exc_info()
