import errno
from eventlet import patcher
from eventlet.support import get_errno
from eventlet.hubs.hub import BaseHub, SYSTEM_EXCEPTIONS

select = patcher.original('select')
ev_sleep = patcher.original('time').sleep

try:
    BAD_SOCK = (errno.EBADF, errno.WSAENOTSOCK)
except AttributeError:
    BAD_SOCK = (errno.EBADF,)


def is_available():
    return hasattr(select, 'select')


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
            self.listeners_events(rs, ws, es)
        except select.error as e:
            if get_errno(e) == errno.EINTR:
                return
            elif get_errno(e) in BAD_SOCK:
                self._remove_bad_fds()
                return
        except SYSTEM_EXCEPTIONS:
            raise
        except:
            return
