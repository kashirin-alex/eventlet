from eventlet.hubs import active_hub
from eventlet.support import greenlets as greenlet
import os

from linuxfd import eventfd_c

__all__ = ['EventFd']

eventfd_create = eventfd_c.eventfd
eventfd_read = eventfd_c.eventfd_read
eventfd_write = eventfd_c.eventfd_write
EV_FLAGS = eventfd_c.EFD_NONBLOCK | eventfd_c.EFD_SEMAPHORE


class EventFd(object):
    __slots__ = ['_fd', '_current']

    def __init__(self, semaphore=True):

        fileno = int(eventfd_create(0, EV_FLAGS if semaphore else eventfd_c.EFD_NONBLOCK))

        self._current = None
        active_hub.inst.mark_as_reopened(fileno)
        listener = active_hub.inst.add(active_hub.inst.READ, fileno, self.switch, self.throw, None)
        self._fd = fileno
        #

    def fileno(self):
        return self._fd
        #

    def dup(self):
        return os.dup(self._fd)
        #

    def switch(self, *args):
        return self._current.switch(*args)
        #

    def throw(self, *args):
        return self._current.throw(*args)
        #

    def receive(self):
        self._current = greenlet.getcurrent()
        assert active_hub.inst.greenlet is not self._current, 'do not call blocking functions from the mainloop'

        active_hub.inst.switch()
        return int(eventfd_read(self._fd))
        #

    def close(self):
        return active_hub.inst.notify_close(self._fd)
        #

    @staticmethod
    def send_count(fd, count=1):
        eventfd_write(fd, count)
    #

