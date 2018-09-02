import errno

from eventlet import patcher
from eventlet.hubs.hub import BaseHub, SYSTEM_EXCEPTIONS
from eventlet.support import get_errno

select = patcher.original('select')


def is_available():
    return hasattr(select, 'poll')

EXC_MASK = select.POLLERR | select.POLLHUP
READ_MASK = select.POLLIN | select.POLLPRI
WRITE_MASK = select.POLLOUT


class Hub(BaseHub):
    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)
        self.poll = select.poll()

    def add(self, evtype, fileno, cb, tb, mac):
        listener = super(Hub, self).add(evtype, fileno, cb, tb, mac)
        self.register(fileno, new=True)
        return listener

    def remove(self, listener):
        super(Hub, self).remove(listener)
        self.register(listener.fileno)

    def register(self, fileno, new=False):
        mask = 0
        if self.listeners_read.get(fileno):
            mask |= READ_MASK | EXC_MASK
        if self.listeners_write.get(fileno):
            mask |= WRITE_MASK | EXC_MASK
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
        super(Hub, self).remove_descriptor(fileno)
        try:
            self.poll.unregister(fileno)
        except (KeyError, ValueError, IOError, OSError):
            # raised if we try to remove a fileno that was
            # already removed/invalid
            pass

    def do_poll(self, seconds):
        # poll.poll expects integral milliseconds
        return self.poll.poll(int(seconds * 1000.0))

    def wait(self, seconds=0):
        try:
            presult = self.do_poll(seconds)
            if not presult:
                return
        except (IOError, select.error) as e:
            if get_errno(e) == errno.EINTR:
                return
            raise
        except SYSTEM_EXCEPTIONS:
            raise
        except:
            return

        listeners = set()
        for fileno, event in presult:
            if event & select.POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            if event & EXC_MASK:
                listeners.add((None, fileno))
                continue
            if event & READ_MASK:
                listeners.add((self.READ, fileno))
            if event & WRITE_MASK:
                listeners.add((self.READ, fileno))
        self.listeners_events(listeners)

