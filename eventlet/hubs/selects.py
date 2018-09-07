import errno
from eventlet import patcher
from eventlet import support
from eventlet.hubs import hub

select = patcher.original('select')
ev_sleep = patcher.original('time').sleep

try:
    BAD_SOCK = (errno.EBADF, errno.WSAENOTSOCK)
except AttributeError:
    BAD_SOCK = (errno.EBADF,)


def is_available():
    return hasattr(select, 'select')

READ = hub.READ
WRITE = hub.WRITE


class Hub(hub.BaseHub):

    def _remove_bad_fds(self):
        """ Iterate through fds, removing the ones that are bad per the
        operating system.
        """
        for fd in list(self.listeners[READ]) + list(self.listeners[WRITE]):
            try:
                select.select([fd], [], [], 0)
            except select.error as e:
                if support.get_errno(e) in BAD_SOCK:
                    self.remove_descriptor(fd)

    def wait(self, seconds=0):
        readers = list(self.listeners[READ])
        writers = list(self.listeners[WRITE])
        if not readers and not writers:
            ev_sleep(seconds)
            return

        try:
            rs, ws, es = select.select(readers, writers, readers + writers, seconds)

            for ev_type, events in ((READ, rs), (WRITE, ws), (None, es)):
                for fileno in events:
                    self.listeners_events.append((ev_type, fileno))

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
