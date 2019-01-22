import errno

from eventlet import patcher
from eventlet.hubs.v1_hub import BaseHub
from eventlet import support

select = patcher.original('select')


def is_available():
    return hasattr(select, 'poll')

CLOSED_MASK = select.POLLNVAL | 0x2000
EXC_MASK = select.POLLERR | select.POLLHUP
READ_MASK = select.POLLIN | select.POLLPRI
WRITE_MASK = select.POLLOUT
POLLNVAL = select.POLLNVAL


class Hub(BaseHub):
    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)
        self.poll = select.poll()

    def add(self, *args):
        """ *args: evtype, fileno, cb, tb, mac """
        listener = self.add_listener(*args)
        self.register(args[1], new=True)
        return listener
        #

    def remove(self, listener):
        self.remove_listener(listener)
        self.register(listener.fileno)
        #

    def register(self, fileno, new=False):
        mask = 0
        if self.has_listener_reader(fileno):
            mask |= READ_MASK | EXC_MASK | CLOSED_MASK
        if self.has_listener_writer(fileno):
            mask |= WRITE_MASK | EXC_MASK | CLOSED_MASK
        try:
            if mask:
                if new:
                    self.poll.register(fileno, mask)
                    return
                try:
                    self.poll.modify(fileno, mask)
                except (IOError, OSError):
                    self.poll.register(fileno, mask)
                return
            try:
                self.poll.unregister(fileno)
            except (KeyError, IOError, OSError):
                # raised if we try to remove a fileno that was
                # already removed/invalid
                pass
        except ValueError:
            # fileno is bad, issue 74
            self.remove_descriptor(fileno)
            raise

    def remove_descriptor(self, fileno):
        self.remove_descriptor_from_listeners(fileno)
        try:
            self.poll.unregister(fileno)
        except (KeyError, ValueError, IOError, OSError):
            # raised if we try to remove a fileno that was
            # already removed/invalid
            pass
        #

    def do_poll(self, seconds):
        # poll.poll expects integral milliseconds
        return self.poll.poll(int(seconds * 1000.0))

    def wait(self, seconds=0):
        try:
            presult = self.do_poll(seconds)
            if not presult:
                return
        except (IOError, select.error) as e:
            if support.get_errno(e) == errno.EINTR:
                return
            raise
        except self.SYSTEM_EXCEPTIONS:
            raise
        except:
            return

        for f, ev in presult:
            if ev & CLOSED_MASK:
                self.remove_descriptor(f)
                continue
            if ev & EXC_MASK or ev & WRITE_MASK:
                self.add_listener_events((self.WRITE, f))
            if ev & EXC_MASK or ev & READ_MASK:
                self.add_listener_events((self.READ, f))
