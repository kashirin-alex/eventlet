import errno

from eventlet import patcher
from eventlet.hubs import hub
from eventlet import support

select = patcher.original('select')


def is_available():
    return hasattr(select, 'poll')

EXC_MASK = select.POLLERR | select.POLLHUP
READ_MASK = select.POLLIN | select.POLLPRI
WRITE_MASK = select.POLLOUT
READ = hub.READ
WRITE = hub.WRITE


class Hub(hub.BaseHub):
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

        if self.listeners[READ].get(fileno):
            mask |= READ_MASK | EXC_MASK
        if self.listeners[WRITE].get(fileno):
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
            if support.get_errno(e) == errno.EINTR:
                return
            raise
        except self.SYSTEM_EXCEPTIONS:
            raise
        except:
            return

        for fileno, event in presult:
            if event & select.POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            if event & EXC_MASK:
                self.listeners_events.append((None, fileno))
                continue
            if event & READ_MASK:
                self.listeners_events.append((READ, fileno))
            if event & WRITE_MASK:
                self.listeners_events.append((WRITE, fileno))

