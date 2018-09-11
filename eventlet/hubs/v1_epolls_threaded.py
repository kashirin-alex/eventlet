import errno

import eventlet
from eventlet import support
from eventlet.hubs.v1_hub_threaded import BaseHub, ev_sleep

select = eventlet.patcher.original('select')
orig_threading = eventlet.patcher.original('threading')


def is_available():
    return hasattr(select, 'epoll')

EXC_MASK = select.POLLERR | select.POLLHUP
READ_MASK = select.POLLIN | select.POLLPRI
WRITE_MASK = select.POLLOUT
POLLNVAL = select.POLLNVAL
SELECT_ERR = select.error


class Hub(BaseHub):
    """ epolls Hub with Threaded Poll Waiter. """

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.poll = select.epoll()
        self.do_poll = self.poll.poll
        self.poll_register = self.poll.register
        self.poll_modify = self.poll.modify
        self.poll_unregister = self.poll.unregister
        #

    def add(self, *args):
        """ *args: evtype, fileno, cb, tb, mac """
        new = not self.has_fileno_listener(args[1])
        listener = self.add_listener(*args)
        try:
            # new=True, Means we've added a new listener
            self.register(args[1], new=new)
        except IOError as ex:    # ignore EEXIST, #80
            if support.get_errno(ex) != errno.EEXIST:
                raise
        return listener
        #

    def register(self, fileno, new=False):
        mask = 0
        if fileno in self.listeners[self.READ]:
            mask |= READ_MASK | EXC_MASK
        if fileno in self.listeners[self.WRITE]:
            mask |= WRITE_MASK | EXC_MASK
        try:
            if mask:
                if new:
                    self.poll_register(fileno, mask)
                    return
                try:
                    self.poll_modify(fileno, mask)
                except (IOError, OSError):
                    self.poll_register(fileno, mask)
                return
            try:
                self.poll_unregister(fileno)
            except (KeyError, IOError, OSError):
                # raised if we try to remove a fileno that was
                # already removed/invalid
                pass
        except ValueError:
            # fileno is bad, issue 74
            self.remove_descriptor(fileno)
            raise
        #

    def remove(self, listener):
        if not listener.spent:
            self.remove_listener(listener)
        self.register(listener.fileno)
        #

    def remove_descriptor(self, fileno):
        self.remove_descriptor_from_listeners(fileno)
        try:
            self.poll_unregister(fileno)
        except (KeyError, ValueError, IOError, OSError):
            # raised if we try to remove a fileno that was
            # already removed/invalid
            pass
        #

    def wait(self, seconds=3):
        try:
            presult = self.do_poll(self.DEFAULT_SLEEP)
        except self.SYSTEM_EXCEPTIONS:
            raise
        except:
            presult = None

        if not presult:
            ev_sleep(seconds)
            return

        for fileno, event in presult:
            if event & POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            if event & EXC_MASK:
                self.add_listener_event((None, fileno))
                continue
            if event & READ_MASK:
                self.add_listener_event((self.READ, fileno))
            if event & WRITE_MASK:
                self.add_listener_event((self.WRITE, fileno))
        #