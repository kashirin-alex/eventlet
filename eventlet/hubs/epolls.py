import errno
from eventlet import support
from eventlet import patcher

from eventlet.hubs.hub import BaseHub
from eventlet.hubs import poll

select = patcher.original('select')


def is_available():
    return hasattr(select, 'epoll')

# NOTE: we rely on the fact that the epoll flag constants
# are identical in value to the poll constants


class Hub(poll.Hub):
    def __init__(self, clock=None):
        BaseHub.__init__(self, clock)
        self.poll = select.epoll()

    def add(self, evtype, fileno, cb, tb, mac):
        new = not (fileno in self.listeners[self.READ] or fileno in self.listeners[self.WRITE])
        listener = BaseHub.add(self, evtype, fileno, cb, tb, mac)
        try:
            # new=True, Means we've added a new listener
            self.register(fileno, new=new)
        except IOError as ex:    # ignore EEXIST, #80
            if support.get_errno(ex) != errno.EEXIST:
                raise
        return listener

    def do_poll(self, seconds):
        return self.poll.poll(seconds)
