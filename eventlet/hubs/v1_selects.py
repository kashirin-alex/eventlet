import errno

from eventlet import patcher
from eventlet.hubs.v1_hub import BaseHub
from eventlet import support

select = patcher.original('select')
ev_sleep = patcher.original('time').sleep


def is_available():
    return hasattr(select, 'select')

try:
    BAD_SOCK = (errno.EBADF, errno.WSAENOTSOCK)
except AttributeError:
    BAD_SOCK = (errno.EBADF,)


class Hub(BaseHub):

    def _remove_bad_fds(self):
        """ Iterate through fds, removing the ones that are bad per the
        operating system.
        """
        for fd in list(self.listeners[self.READ]) + list(self.listeners[self.WRITE]):
            try:
                select.select([fd], [], [], 0)
            except select.error as e:
                if support.get_errno(e) in BAD_SOCK:
                    self.remove_descriptor(fd)

    def wait(self, seconds=0):
        readers = list(self.listeners[self.READ])
        writers = list(self.listeners[self.WRITE])
        if not readers and not writers:
            ev_sleep(seconds)
            return

        try:
            rs, ws, es = select.select(readers, writers, readers + writers, seconds)
            for add_event, events in ((self.add_fd_event_read, rs),
                                      (self.add_fd_event_write, ws),
                                      (self.add_fd_event_error, es)):
                for fileno in events:
                    add_event(fileno)

        except select.error as e:
            errn = support.get_errno(e)
            if errn == errno.EINTR:
                return
            elif errn in BAD_SOCK:
                self._remove_bad_fds()
                return
        except self.SYSTEM_EXCEPTIONS:
            raise
        except:
            return
