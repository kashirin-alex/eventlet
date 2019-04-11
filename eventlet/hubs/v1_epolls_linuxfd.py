import errno
import os
import sys
import traceback

from linuxfd import timerfd_c, eventfd_c

import eventlet
from eventlet.support import clear_sys_exc_info, get_errno
from eventlet.hubs.v1_skeleton import HubSkeleton

select = eventlet.patcher.original('select')


def is_available():
    return hasattr(select, 'epoll')

SYSTEM_EXCEPTIONS = HubSkeleton.SYSTEM_EXCEPTIONS
# FD-TYPES:
FILE = 0
TIMER = 1
EVENT = 2

# FILE-FD DETAILS:
READ = 0
WRITE = 1
EVENT_TYPES = (READ, WRITE)

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
    WRITE = WRITE
    READ = READ

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.fds = {}
        # fds map, fileno: tuple(fd-type, type-dependent)
        #                        FILE   , dict{evtype: listener}
        #                        TIMER  , timer-obj
        #                        EVENT  , callback
        self.secondaries = ({}, {})
        self.closed = []

        self.timers_immediate = []
        self.add_immediate_timer = self.timers_immediate.append

        self.poll = select.epoll()
        self.poll_backing = select.epoll.fromfd(os.dup(self.poll.fileno()))
        #

    def add_timer(self, timer):
        seconds = timer.seconds
        if seconds < MIN_TIMER:
            self.add_immediate_timer(timer)
            return timer

        fileno = int(timerfd_create(TIMER_CLOCK, TIMER_FLAGS))
        timer.fileno = fileno
        self.fds[fileno] = (TIMER, timer)
        try:
            self.poll.register(fileno, TIMER_MASK)
        except:
            # delayed in registering followed expired and closed timer fd
            del self.fds[fileno]
            timer.seconds = 0  # pass-through
            return self.add_timer(timer)  # really bad, if can't make a timer
        # zero and below 1 ns disarms a timer
        timerfd_settime(fileno, 0, seconds, 0)
        return timer
        #

    def timer_canceled(self, timer):
        fileno = timer.fileno
        if self.fds.pop(fileno, None) is None:
            return False
        try:
            timerfd_settime(fileno, 0, 0, 0)
            self.poll.unregister(fileno)
            os.close(fileno)
        except:
            pass
        #

    def event_add(self, cb, semaphore=True):
        fileno = int(eventfd_create(0, EV_FLAGS if semaphore else eventfd_c.EFD_NONBLOCK))
        self.fds[fileno] = (EVENT, cb)
        self.poll.register(fileno, EV_READ_MASK)
        return fileno
        #

    def event_close(self, fileno):
        if self.fds.pop(fileno, None) is None:
            return False
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
        found = False
        for listener in self.secondaries[READ].pop(fileno, [])+self.secondaries[WRITE].pop(fileno, []):
            found = True
            self.closed.append(listener)
            listener.defang()

        # For the primary listeners, we actually need to call remove,
        # which may modify the underlying OS polling objects.
        fd = self.fds.get(fileno)
        if fd is None or fd[0] != FILE:
            return found

        for evtype in EVENT_TYPES:
            listener = fd[1].get(evtype)
            if listener is None:
                continue
            found = True
            self.closed.append(listener)
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

        timers_immediate = self.timers_immediate

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
            events = self.poll.poll(0 if timers_immediate else -1)
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

        get_fd = self.fds.get
        for f, ev in events:
            desc = get_fd(f)
            if desc is None:  # print ('poll has unknown fileno', f)
                continue

            typ, details = desc
            if typ == EVENT:
                try:
                    details(int(eventfd_read(f)))
                except OSError as e:
                    if get_errno(e) == errno.EBADF:
                        details(-1)  # recreate handler?
                # except io.BlockingIOError: -- write is not done with switch
                #    pass  # value is above/below int64
                except:
                    pass
                continue

            if typ == TIMER:
                try:
                    self.poll.unregister(f)
                    os.close(f)  # release resources first
                except:
                    pass
                try:
                    del self.fds[f]
                    details()  # exec timer
                except:
                    pass
                continue

            if typ == FILE:
                try:
                    if ev & READ_MASK and READ in details:
                        details[READ].cb(f)
                    if ev & WRITE_MASK and WRITE in details:
                        details[WRITE].cb(f)
                        if ev & EPOLLRDHUP:
                            self._obsolete(f)
                    if ev & CLOSED_MASK:
                        self.remove_descriptor(f)
                except SYSTEM_EXCEPTIONS:
                    continue
                except:
                    self.squelch_exception(f, sys.exc_info())
                    clear_sys_exc_info()
                continue

        while self.closed:  # Ditch all closed fds first.
            l = self.closed.pop(-1)
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
            while self.fds:
                fileno, fd = self.fds.popitem()
                try:
                    if fd[0] == TIMER:
                        timerfd_settime(fileno, 0, 0, 0)
                    self.poll.unregister(fileno)
                    os.close(fileno)
                except:
                    pass
            del self.closed[:]

            for ev in EVENT_TYPES:
                self.secondaries[ev].clear()
            self.poll.close()
            self.poll_backing.close()
            self.stopping = False
            self.running = False
        #

    def get_readers(self):
        return len([None for fileno in self.fds
                    if self.fds[fileno][0] == FILE and READ in self.fds[fileno][1]])
        #

    def get_writers(self):
        return len([None for fileno in self.fds
                    if self.fds[fileno][0] == FILE and WRITE in self.fds[fileno][1]])
        #

    def get_timers_count(self):
        return len([None for fileno in self.fds if self.fds[fileno][0] == TIMER])
        #

    def get_listeners_count(self):
        return len([None for fileno in self.fds if self.fds[fileno][0] == FILE])
        #

    def add(self, evtype, fileno, cb, tb, mac):
        """ *args: evtype, fileno, cb, tb, mac """
        # evtype, fileno = args[0:2]

        fd = self.fds.get(fileno)

        listener = self.lclass(evtype, fileno, cb, tb, mac)
        if fd is not None:
            if evtype in fd[1]:
                if self.g_prevent_multiple_readers:
                    raise RuntimeError(
                        "Second simultaneous %s on fileno %s "
                        "detected.  Unless you really know what you're doing, "
                        "make sure that only one greenthread can %s any "
                        "particular socket.  Consider using a pools.Pool. "
                        "If you do know what you're doing and want to disable "
                        "this error, call "
                        "eventlet.debug.hub_prevent_multiple_readers(False) - MY THREAD=%s; "
                        "THAT THREAD=%s" % (evtype, fileno, evtype, cb, fd[1][evtype]))
                # store off the second listener in another structure
                self.secondaries[evtype].setdefault(fileno, []).append(listener)
            else:
                fd[1][evtype] = listener
            self.modify(fileno, fd)
        else:
            self.fds[fileno] = (FILE, {evtype: listener})
            self.poll.register(fileno, READ_MASK if evtype == READ else WRITE_MASK)

        return listener
        #

    def remove(self, listener):
        fileno = listener.fileno
        fd = self.fds.get(fileno)
        if fd is None or fd[0] != FILE:
            return

        if not listener.spent:
            evtype = listener.evtype
            sec = self.secondaries[evtype].get(fileno)
            if not sec:
                fd[1].pop(evtype, None)
            else:
                # migrate a secondary listener to be the primary listener
                fd[1][evtype] = sec.pop(0)
                if not sec:
                    del self.secondaries[evtype][fileno]

        if not fd[1]:
            del self.fds[fileno]
            try:
                self.poll.unregister(fileno)
            except:
                pass
            return
        self.modify(fileno, fd)
        #

    def modify(self, fileno, fd):
        mask = 0
        if READ in fd[1]:
            mask |= READ_MASK
        if WRITE in fd[1]:
            mask |= WRITE_MASK
        self.poll.modify(fileno, mask)
        #

    def remove_descriptor(self, fileno):
        """ Completely remove all listeners for this fileno.  For internal use
        only."""
        fd = self.fds.pop(fileno, None)
        if fd is None:
            return
        for l in [fd[1][evtype] for evtype in fd[1]] if fd is not None else [] \
                + self.secondaries[READ].pop(fileno, []) \
                + self.secondaries[WRITE].pop(fileno, []):
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
