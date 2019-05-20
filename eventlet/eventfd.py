from eventlet.hubs import trampoline, notify_opened, notify_close
import os

from linuxfd import eventfd_c

__all__ = ['EventFd']

eventfd_create = eventfd_c.eventfd
eventfd_read = eventfd_c.eventfd_read
eventfd_write = eventfd_c.eventfd_write
EV_FLAGS = eventfd_c.EFD_NONBLOCK | eventfd_c.EFD_SEMAPHORE


class EventFd(object):
    __slots__ = ['_fd']

    def __init__(self, semaphore=True):
        self._fd = int(eventfd_create(0, EV_FLAGS if semaphore else eventfd_c.EFD_NONBLOCK))
        notify_opened(self._fd)
        #

    def fileno(self):
        return self._fd
        #

    def dup(self):
        return os.dup(self._fd)
        #

    def receive(self):
        trampoline(self._fd, read=True)
        return int(eventfd_read(self._fd))
        #

    def close(self):
        notify_close(self._fd)
        try:
            os.close(self._fd)
        except:
            pass
        #

    @staticmethod
    def send_count(fd, count=1):
        eventfd_write(fd, count)
    #

