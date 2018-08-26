import errno
import sys

from eventlet import patcher
from eventlet.hubs.hub import BaseHub, READ, WRITE, noop
from eventlet.support import get_errno, clear_sys_exc_info

select = patcher.original('select')
time = patcher.original('time')

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
        if self.readers.get(fileno):
            mask |= READ_MASK | EXC_MASK
        if self.writers.get(fileno):
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

    def wait(self, seconds=None):
        if not self.readers and not self.writers:
            if not seconds:
                return
        try:
            presult = self.do_poll(seconds)
        except (IOError, select.error) as e:
            if get_errno(e) == errno.EINTR:
                return
            raise

        if self.debug_blocking:
            self.block_detect_pre()

        # Accumulate the listeners to call back to prior to
        # triggering any of them. This is to keep the set
        # of callbacks in sync with the events we've just
        # polled for. It prevents one handler from invalidating
        # another.
        callbacks = set()
        for fileno, event in presult:
            if event & select.POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            if event & READ_MASK:
                l = self.readers.get(fileno)
                if l:
                    callbacks.add((l, fileno))
            if event & WRITE_MASK:
                l = self.writers.get(fileno)
                if l:
                    callbacks.add((l, fileno))
            if event & EXC_MASK:
                l = self.readers.get(fileno)
                if l:
                    callbacks.add((l, fileno))
                l = self.writers.get(fileno)
                if l:
                    callbacks.add((l, fileno))

        sys_exceptions = self.SYSTEM_EXCEPTIONS
        for listener, fileno in callbacks:
            try:
                listener.cb(fileno)
            except sys_exceptions:
                raise
            except:
                self.squelch_exception(fileno, sys.exc_info())
                clear_sys_exc_info()

        if self.debug_blocking:
            self.block_detect_post()
