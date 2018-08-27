import errno
from eventlet.support import get_errno
from eventlet import patcher

from eventlet.hubs.hub import BaseHub
from eventlet.hubs import poll

select = patcher.original('select')
if not hasattr(select, 'epoll'):
    # TODO: remove mention of python-epoll on 2019-01
    raise ImportError('No epoll implementation found in select module.'
                      ' python-epoll (or similar) package support was removed,'
                      ' please open issue on https://github.com/eventlet/eventlet/'
                      ' if you must use epoll outside stdlib.')

# NOTE: we rely on the fact that the epoll flag constants
# are identical in value to the poll constants


class Hub(poll.Hub):
    def __init__(self, clock=None):
        BaseHub.__init__(self, clock)
        self.poll = select.epoll()

    def add(self, evtype, fileno, cb, tb, mac):
        new = not (fileno in self.listeners_read or fileno in self.listeners_write)
        listener = BaseHub.add(self, evtype, fileno, cb, tb, mac)
        try:
            # Means we've added a new listener
            self.register(fileno, new=new)
        except IOError as ex:    # ignore EEXIST, #80
            if get_errno(ex) != errno.EEXIST:
                raise
        return listener

    def do_poll(self, seconds):
        return self.poll.poll(seconds)
