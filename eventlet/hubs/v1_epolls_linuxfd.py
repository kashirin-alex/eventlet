import errno
import os
import sys
import traceback

from linuxfd import timerfd_c

import eventlet
from eventlet.support import clear_sys_exc_info, get_errno
from eventlet.hubs.v1_skeleton import HubSkeleton, HubFileDetails

select = eventlet.patcher.original('select')


def is_available():
    return hasattr(select, 'epoll')


SYSTEM_EXCEPTIONS = HubSkeleton.SYSTEM_EXCEPTIONS

# FILE-FD DETAILS:
READ = 0
WRITE = 1

EPOLLRDHUP = 0x2000
CLOSED_MASK = select.POLLNVAL
EXC_MASK = select.EPOLLERR | select.EPOLLHUP

READ_MASK = select.EPOLLIN | select.EPOLLPRI | EXC_MASK
WRITE_MASK = select.EPOLLOUT | EXC_MASK | EPOLLRDHUP

# TIMER-FD DETAILS:
MIN_TIMER = 0.000000001
TIMER_MASK = select.EPOLLIN | select.EPOLLONESHOT | EXC_MASK

TIMER_CLOCK = timerfd_c.CLOCK_MONOTONIC
TIMER_FLAGS = timerfd_c.TFD_NONBLOCK
timerfd_create = timerfd_c.timerfd_create
timerfd_settime = timerfd_c.timerfd_settime


class Hub(HubSkeleton):
    __slots__ = HubSkeleton.__slots__ + ['fds', 'closed',  'timers_immediate', 'poll', 'poll_backing']
    WRITE = WRITE
    READ = READ

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.fds = {}        # HubFileDetails
        self.closed = []     # FdListener

        self.timers_immediate = []  # timer-obj

        self.poll = select.epoll()
        self.poll_backing = select.epoll.fromfd(os.dup(self.poll.fileno()))
        #

    def add_timer(self, timer):
        seconds = timer.seconds
        if seconds < MIN_TIMER:
            self.timers_immediate.append(timer)
            return timer

        fileno = int(timerfd_create(TIMER_CLOCK, TIMER_FLAGS))
        self._obsolete(fileno)

        timer.fileno = fileno
        self.fds[fileno] = HubFileDetails(timer, True)
        try:
            self.poll.register(fileno, TIMER_MASK)
        except:
            # delayed in registering followed expired and closed timer fd
            self.timer_canceled(timer)
            timer.seconds = 0  # pass-through
            return self.add_timer(timer)  # really bad, if can't make a timer
        # zero and below 1 ns disarms a timer
        timerfd_settime(fileno, 0, seconds, 0)
        return timer
        #

    def timer_canceled(self, timer):
        fileno = timer.fileno
        fd = self.fds.pop(fileno, None)
        if fd is None:
            return
        try:
            self.poll.unregister(fileno)
        except:
            pass
        try:
            timerfd_settime(fileno, 0, 0, 0)
        except:
            pass
        try:
            os.close(fileno)
        except:
            pass
        #

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """
        fd = self.fds.pop(fileno, ())
        if not fd:
            return

        try:
            self.poll.unregister(fileno)
        except:
            pass

        for listener in fd:
            self.closed.append(listener)
            listener.defang()
        #

    def notify_close(self, fileno):
        self._obsolete(fileno)
        #

    def mark_as_reopened(self, fileno):
        """ If a file descriptor is returned by the OS as the result of some
            open call (or equivalent), that signals that it might be being
            recycled.
            Catch the case where the fd was previously in use.
        """
        self._obsolete(fileno)
        #

    def squelch_exception(self, fileno, exc_info):
        traceback.print_exception(*exc_info)
        sys.stderr.write("Removing descriptor: %r\n" % (fileno,))
        sys.stderr.flush()
        try:
            self._obsolete(fileno)
        except Exception as e:
            sys.stderr.write("Exception while removing descriptor! %r\n" % (e,))
            sys.stderr.flush()
        #

    def ditch_closed(self):
        while self.closed:  # Ditch all closed fds.
            l = self.closed.pop(0)
            if not hasattr(l, 'greenlet') or (l.greenlet and not l.greenlet.dead):
                # There's no point signalling a greenlet that's already dead. and Timer has it's called state
                l.tb(eventlet.hubs.IOClosed(errno.ENOTCONN, "Operation on closed file"))
        #

    def execute_polling(self):
        self.ditch_closed()

        timers_immediate = self.timers_immediate
        if timers_immediate:
            immediate = timers_immediate[:]  # copy current and exec without new to come
            del timers_immediate[:]
            for t in immediate:
                try:
                    t()  # exec immediate timer
                except:
                    pass
        try:
            events = self.poll.poll(0 if timers_immediate else -1)
            if not events or not self.fds:
                if events and not self.fds:
                    print ('WARN', 'no fds map for', events)
                return True
        except ValueError:
            if not self.stopping:
                try:
                    self.poll.close()
                except:
                    pass
                self.poll = self.poll_backing
                self.poll_backing = select.epoll.fromfd(os.dup(self.poll.fileno()))
                return True
            return False
        except Exception as e:
            print (e, get_errno(e))
            return True

        # use prior details,
        # a FD can be cancelled and a new created with the same filno which can't be on the current evs poll

        fds = self.fds
        for f, ev, details in [(f, ev, fds.get(f)) for f, ev in events if f in fds]:
            try:
                if ev & READ_MASK and details.rs:
                    details.rs[0]()
                if ev & WRITE_MASK and details.ws:
                    details.ws[0]()
            except SYSTEM_EXCEPTIONS:
                continue
            except:
                self.squelch_exception(f, sys.exc_info())
                clear_sys_exc_info()
                continue
            if ev & CLOSED_MASK or ev & EPOLLRDHUP:
                self._obsolete(f)
        return True
        #

    def run(self, *a, **kw):
        """Run the runloop until abort is called.
        """
        # accept and discard variable arguments because they will be
        # supplied if other greenlets have run and exited before the
        # hub's greenlet gets a chance to run
        if self.running:
            raise RuntimeError("Already running!")

        self.running = True
        self.stopping = False

        try:
            while True:
                if not self.execute_polling():
                    break
        finally:

            while self.fds:
                self._obsolete(self.fds.keys()[0])
            self.ditch_closed()

            self.poll.close()
            self.poll_backing.close()
            self.stopping = False
            self.running = False
        #

    def get_readers(self):
        return sum([len(self.fds[fileno].rs) for fileno in self.fds
                    if self.fds[fileno].rs and isinstance(self.fds[fileno].rs[0], self.lclass)])
        #

    def get_writers(self):
        return sum([len(self.fds[fileno].ws) for fileno in self.fds
                    if self.fds[fileno].ws and isinstance(self.fds[fileno].ws[0], self.lclass)])
        #

    def get_timers_count(self):
        return len([None for fileno in self.fds
                    if self.fds[fileno].rs and not isinstance(self.fds[fileno].rs[0], self.lclass)])
        #

    def get_listeners_count(self):
        return sum([len(self.fds[fileno]) for fileno in self.fds])
        #

    def add(self, *args):
        """ *args: evtype, fileno, cb, tb, mac """
        evtype, fileno = args[:2]
        fd = self.fds.get(fileno)
        listener = self.lclass(*args)

        if fd is not None:
            fd.add(listener, evtype == READ, self.g_prevent_multiple_readers)
            self.modify(fileno, fd)
        else:
            self.fds[fileno] = HubFileDetails(listener, evtype == READ)
            self.poll.register(fileno, READ_MASK if evtype == READ else WRITE_MASK)

        return listener
        #

    def modify(self, fileno, details):
        mask = 0
        if details.rs:
            mask |= READ_MASK
        if details.ws:
            mask |= WRITE_MASK

        self.poll.modify(fileno, mask)
        #

    def remove(self, listener):
        fileno = listener.fileno
        fd = self.fds.get(fileno)
        if fd is None:
            return

        fd.remove(listener, listener.evtype == READ)
        self.modify(fileno, fd)
        #
