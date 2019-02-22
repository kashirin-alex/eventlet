
from eventlet.hubs import active_hub
from eventlet.support import greenlets as greenlet
import os
from linuxfd.eventfd_c import eventfd_write


__all__ = ['EventFd']


class EventFd(object):
    __slots__ = ['_fd', '_switch']

    def __init__(self, semaphore=True):
        self._fd = active_hub.inst.event_add(self._switcher, semaphore)
        self._switch = None
        #

    def fileno(self):
        return self._fd
        #

    def dup(self):
        return os.dup(self._fd)
        #

    def receive(self):
        current = greenlet.getcurrent()
        assert active_hub.inst.greenlet is not current, 'do not call blocking functions from the mainloop'

        self._switch = current.switch
        return active_hub.inst.switch()

    def _switcher(self, *args):
        return self._switch(*args)
        #

    def close(self):
        return active_hub.inst.event_close(self._fd)
        #

    @staticmethod
    def send_count(fd, count=1):
        eventfd_write(fd, count)
    #

