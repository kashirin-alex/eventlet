import errno
import os
import sys
import traceback

from linuxfd import timerfd_c, eventfd_c

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
TIMER_MASK = select.EPOLLIN | select.EPOLLONESHOT

TIMER_CLOCK = timerfd_c.CLOCK_MONOTONIC
TIMER_FLAGS = timerfd_c.TFD_NONBLOCK
timerfd_create = timerfd_c.timerfd_create
timerfd_settime = timerfd_c.timerfd_settime

# EVENT-FD DETAILS:
eventfd_create = eventfd_c.eventfd
eventfd_read = eventfd_c.eventfd_read
EV_FLAGS = eventfd_c.EFD_NONBLOCK | eventfd_c.EFD_SEMAPHORE
EV_READ_MASK = select.EPOLLIN | select.EPOLLPRI


class Hub(HubSkeleton):
    __slots__ = HubSkeleton.__slots__ + \
                ['fds', 'closed', 'fd_events', 'fd_timers', 'timers_immediate', 'poll', 'poll_backing']
    WRITE = WRITE
    READ = READ

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.fds = {}        # HubFileDetails
        self.closed = []     # FdListener

        self.fd_events = {}  # timer-obj
        self.fd_timers = {}  # callback
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
        timer.fileno = fileno
        self.fd_timers[fileno] = timer
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
        if self.fd_timers.pop(fileno, None) is None:
            return
        try:
            timerfd_settime(fileno, 0, 0, 0)
            self.poll.unregister(fileno)
            os.close(fileno)
        except:
            pass
        #

    def event_add(self, cb, semaphore=True):
        fileno = int(eventfd_create(0, EV_FLAGS if semaphore else eventfd_c.EFD_NONBLOCK))
        self.fd_events[fileno] = cb
        try:
            self.poll.register(fileno, EV_READ_MASK)
        except Exception as e:
            self.event_close(fileno)
            raise e
        return fileno
        #

    def event_close(self, fileno):
        if self.fd_events.pop(fileno, None) is None:
            return
        try:
            self.poll.unregister(fileno)
            os.close(fileno)
        except:
            pass
        return True
        #

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """
        fd = self.fds.pop(fileno, None)
        if fd is None:
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

    def execute_polling(self):

        while self.closed:  # Ditch all closed fds first.
            l = self.closed.pop(0)
            if not l.greenlet.dead:  # There's no point signalling a greenlet that's already dead.
                l.tb(eventlet.hubs.IOClosed(errno.ENOTCONN, "Operation on closed file"))

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
            if not events:
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

        # in any case, separate events iterations for each type
        # use prior details,
        # a FD can be cancelled and a new created with the same filno which can't be on the current evs poll

        fds = self.fds
        if fds:
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
            #

        fd_events = self.fd_events
        if fd_events:
            for f, cb in [(f, fd_events.get(f)) for f, ev in events if f in fd_events]:
                try:
                    cb(int(eventfd_read(f)))
                except OSError as e:
                    if get_errno(e) == errno.EBADF:
                        cb(-1)  # recreate handler?
                # except io.BlockingIOError: -- write is not done with switch
                #    pass  # value is above/below int64
                except:
                    pass
            #

        fd_timers = self.fd_timers
        if fd_timers:
            for t in [fd_timers.get(f) for f, ev in events if f in fd_timers]:
                try:
                    self.timer_canceled(t)
                    t()
                except:
                    pass
            #

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
            while self.fd_timers:
                self.timer_canceled(self.fd_timers.values()[0])
            while self.fd_events:
                self.event_close(self.fd_events.keys()[0])

            del self.closed[:]
            self.poll.close()
            self.poll_backing.close()
            self.stopping = False
            self.running = False
        #

    def get_readers(self):
        return sum([len(self.fds[fileno].rs) for fileno in self.fds])
        #

    def get_writers(self):
        return sum([len(self.fds[fileno].ws) for fileno in self.fds])
        #

    def get_timers_count(self):
        return len(self.fd_timers)
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
