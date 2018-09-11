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
        #

    def add(self, *args):
        """ *args: evtype, fileno, cb, tb, mac """
        new = not self.has_listeners_fileno(args[1])
        listener = self.add_listener(*args)
        try:
            self.register(args[1], new=new)
        except IOError as ex:    # ignore EEXIST, #80
            if support.get_errno(ex) != errno.EEXIST:
                raise
        return listener
        #

    def register(self, fileno, new=False):
        mask = 0
        if self.has_listener_reader(fileno):
            mask |= READ_MASK | EXC_MASK
        if self.has_listener_writer(fileno):
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
        #

    def remove(self, listener):
        if not listener.spent:
            self.remove_listener(listener)
        self.register(listener.fileno)
        #

    def remove_descriptor(self, fileno):
        self.remove_descriptor_from_listeners(fileno)
        try:
            self.poll.unregister(fileno)
        except (KeyError, ValueError, IOError, OSError):
            # raised if we try to remove a fileno that was
            # already removed/invalid
            pass
        #

    def wait(self, seconds=3):
        try:
            presult = self.poll.poll(self.DEFAULT_SLEEP)
            if not presult:
                ev_sleep(3)
                return
        except (IOError, select.error) as e:
            if support.get_errno(e) == errno.EINTR:
                ev_sleep(3)
                return
            raise
        except self.SYSTEM_EXCEPTIONS:
            raise
        except:
            ev_sleep(3)
            return

        for fileno, event in presult:
            if event & POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            if event & EXC_MASK:
                self.add_fd_event_error(fileno)
                continue
            if event & READ_MASK:
                self.add_fd_event_read(fileno)
            if event & WRITE_MASK:
                self.add_fd_event_write(fileno)
        #
