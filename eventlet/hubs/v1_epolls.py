import errno
from eventlet import support
from eventlet import patcher

from eventlet.hubs.v1_hub import BaseHub
from eventlet.hubs import v1_poll

select = patcher.original('select')


def is_available():
    return hasattr(select, 'epoll')


class Hub(v1_poll.Hub):

    def __init__(self, clock=None):
        BaseHub.__init__(self, clock)
        self.poll = select.epoll()

    def add(self, *args):
        """ *args: evtype, fileno, cb, tb, mac """
        new = not self.has_listeners_fileno(args[1])
        listener = self.add_listener(*args)
        try:
            self.register(args[1], new=new)
        except IOError as ex:
            # ignore EEXIST, #80
            if support.get_errno(ex) != errno.EEXIST:
                raise
        return listener

    def do_poll(self, seconds):
        return self.poll.poll(seconds)
