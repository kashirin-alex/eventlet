import errno
import os
import sys
import traceback

from linuxfd import timerfd_c

import eventlet
from eventlet.support import clear_sys_exc_info, get_errno
from eventlet.hubs.v1_skeleton import HubSkeleton

select = eventlet.patcher.original('select')


def is_available():
    return hasattr(select, 'epoll')

# EVENT TYPE INDEX FOR listeners TUPLE
READ = 0
WRITE = 1
EVENT_TYPES = (READ, WRITE)
MIN_TIMER = 0.000000001

SYSTEM_EXCEPTIONS = HubSkeleton.SYSTEM_EXCEPTIONS

EPOLLRDHUP = 0x2000
CLOSED_MASK = select.POLLNVAL
EXC_MASK = select.EPOLLERR | select.EPOLLHUP

READ_MASK = select.EPOLLIN | select.EPOLLPRI | EXC_MASK
WRITE_MASK = select.EPOLLOUT | EXC_MASK | EPOLLRDHUP
TIMER_MASK = select.EPOLLIN | select.EPOLLONESHOT

TIMER_CLOCK = timerfd_c.CLOCK_MONOTONIC
TIMER_FLAGS = timerfd_c.TFD_NONBLOCK
timer_create = timerfd_c.timerfd_create
timer_settime = timerfd_c.timerfd_settime


class Hub(HubSkeleton):
    WRITE = WRITE
    READ = READ

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.listeners = ({}, {})
        self.listeners_r = self.listeners[READ]
        self.listeners_w = self.listeners[WRITE]
        self.get_reader = self.listeners_r.get
        self.get_writer = self.listeners_w.get

        self.secondaries = ({}, {})
        self.pop_sec_reader = self.secondaries[READ].pop
        self.pop_sec_writer = self.secondaries[WRITE].pop

        self.timers = {}
        self.pop_timer = self.timers.pop

        self.timers_immediate = []
        self.add_immediate_timer = self.timers_immediate.append

        self.closed = []
        self.add_closed = self.closed.append

        self.poll = select.epoll()
        self.poll_backing = select.epoll.fromfd(os.dup(self.poll.fileno()))
        #

    def add_timer(self, timer):
        seconds = timer.seconds
        if seconds == 0:
            self.add_immediate_timer(timer)
            return timer
        elif seconds < MIN_TIMER:  # zero and below 1 ns disarms a timer
            seconds = MIN_TIMER

        fileno = int(timer_create(TIMER_CLOCK, TIMER_FLAGS))
        timer.fileno = fileno
        self.timers[fileno] = timer
        try:
            self.poll.register(fileno, TIMER_MASK)
        except:
            # delayed in registering followed expired and closed timer fd
            self.timers.pop(fileno, None)
            timer.seconds = 0  # pass-through
            return self.add_timer(timer)
        timer_settime(fileno, 0, seconds, 0)
        return timer
        #

    def timer_canceled(self, timer):
        fileno = timer.fileno
        try:
            timer_settime(fileno, 0, 0, 0)
            self.poll.unregister(fileno)
            os.close(fileno)
        except:
            pass
        self.pop_timer(fileno, None)
        #

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """
        found = False
        for listener in self.pop_sec_reader(fileno, [])+self.pop_sec_writer(fileno, []):
            found = True
            self.add_closed(listener)
            listener.defang()

        # For the primary listeners, we actually need to call remove,
        # which may modify the underlying OS polling objects.
        listener = self.get_reader(fileno)
        if listener:
            found = True
            self.add_closed(listener)
            self.remove(listener)
            listener.defang()
        listener = self.get_writer(fileno)
        if listener:
            found = True
            self.add_closed(listener)
            self.remove(listener)
            listener.defang()
        return found
        #

    @staticmethod
    def notify_close(fileno):
        """ We might want to do something when a fileno is closed.
            However, currently it suffices to obsolete listeners only
            when we detect an old fileno being recycled, on open.
        """
        pass
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
            self.remove_descriptor(fileno)
        except Exception as e:
            sys.stderr.write("Exception while removing descriptor! %r\n" % (e,))
            sys.stderr.flush()
        #

    def execute_polling(self):

        timers = self.timers
        pop_timer = self.pop_timer
        timers_immediate = self.timers_immediate

        get_reader = self.get_reader
        get_writer = self.get_writer

        if timers_immediate:
            immediate = timers_immediate[:]  # copy current and exec without new to come
            del timers_immediate[:]
            for t in immediate:
                if t.called:
                    continue
                try:
                    t()  # exec immediate timer
                except:
                    pass
        try:
            fd_events = self.poll.poll(0 if timers_immediate else -1)
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

        for f, ev in fd_events:
            if f in timers:
                try:
                    self.poll.unregister(f)
                    os.close(f)  # release resources first
                except:
                    pass
                try:
                    t = pop_timer(f)
                    if not t.called:
                        t()  # exec timer
                except:
                    pass
                continue

            try:
                if ev & READ_MASK:
                    l = get_reader(f)
                    if l is not None:
                        l.cb(f)
                if ev & WRITE_MASK:
                    l = get_writer(f)
                    if l is not None:
                        l.cb(f)
                    if ev & EPOLLRDHUP:
                        self._obsolete(f)
                if ev & CLOSED_MASK:
                    self.remove_descriptor(f)

            except:
                self.squelch_exception(f, sys.exc_info())
                clear_sys_exc_info()

        closed = self.closed
        pop_closed = self.closed.pop
        while closed:  # Ditch all closed fds first.
            l = pop_closed(-1)
            if not l.greenlet.dead:  # There's no point signalling a greenlet that's already dead.
                l.tb(eventlet.hubs.IOClosed(errno.ENOTCONN, "Operation on closed file"))
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
            while self.timers:
                try:
                    f, t = self.timers.popitem()
                    timer_settime(f, 0, 0, 0)
                    self.poll.unregister(f)
                    os.close(f)
                except:
                    pass

            self.timers.clear()
            del self.closed[:]
            for ev in EVENT_TYPES:
                self.listeners[ev].clear()
                self.secondaries[ev].clear()
            self.poll.close()
            self.stopping = False
            self.running = False
        #

    def get_readers(self):
        return self.listeners_r.values()
        #

    def get_writers(self):
        return self.listeners_w.values()
        #

    def get_timers_count(self):
        return len(self.timers)
        #

    def get_listeners_count(self):
        return len(self.listeners_r),  len(self.listeners_w)
        #

    def add(self, evtype, fileno, cb, tb, mac):
        """ *args: evtype, fileno, cb, tb, mac """
        # evtype, fileno = args[0:2]
        exists = (fileno in self.listeners_r or fileno in self.listeners_w)

        listener = self.lclass(evtype, fileno, cb, tb, mac)
        bucket = self.listeners[evtype]
        if exists and fileno in bucket:
            if self.g_prevent_multiple_readers:
                raise RuntimeError(
                    "Second simultaneous %s on fileno %s "
                    "detected.  Unless you really know what you're doing, "
                    "make sure that only one greenthread can %s any "
                    "particular socket.  Consider using a pools.Pool. "
                    "If you do know what you're doing and want to disable "
                    "this error, call "
                    "eventlet.debug.hub_prevent_multiple_readers(False) - MY THREAD=%s; "
                    "THAT THREAD=%s" % (
                        evtype, fileno, evtype, cb, bucket[fileno]))
            # store off the second listener in another structure
            self.secondaries[evtype].setdefault(fileno, []).append(listener)
        else:
            bucket[fileno] = listener

        try:
            self.register(fileno, new=not exists)
        except IOError as ex:
            # ignore EEXIST, #80
            if get_errno(ex) != errno.EEXIST:
                raise
        return listener
        #

    def register(self, fileno, new=False):
        mask = 0
        if fileno in self.listeners_r:
            mask |= READ_MASK
        if fileno in self.listeners_w:
            mask |= WRITE_MASK
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
        fileno = listener.fileno
        if not listener.spent:
            evtype = listener.evtype
            sec = self.secondaries[evtype].get(fileno)
            if not sec:
                self.listeners[evtype].pop(fileno, None)
            else:
                # migrate a secondary listener to be the primary listener
                self.listeners[evtype][fileno] = sec.pop(0)
                if not sec:
                    self.secondaries[evtype].pop(fileno)
        self.register(fileno)
        #

    def remove_descriptor(self, fileno):
        """ Completely remove all listeners for this fileno.  For internal use
        only."""
        for l in [self.listeners_r.pop(fileno, None)] + [self.listeners_w.pop(fileno, None)] + \
                self.pop_sec_reader(fileno, []) + self.pop_sec_writer(fileno, []):
            if l is None:
                continue

            try:
                l.cb(fileno)
            except:
                self.squelch_exception(fileno, sys.exc_info())
                clear_sys_exc_info()
        try:
            self.poll.unregister(fileno)
        except (KeyError, ValueError, IOError, OSError):
            # raised if we try to remove a fileno that was
            # already removed/invalid
            pass
        #
