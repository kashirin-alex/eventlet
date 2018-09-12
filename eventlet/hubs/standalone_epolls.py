import errno
import sys
import traceback
import heapq

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
DEFAULT_SLEEP = 60.0

heappush = heapq.heappush
heappop = heapq.heappop
SYSTEM_EXCEPTIONS = HubSkeleton.SYSTEM_EXCEPTIONS

EXC_MASK = select.POLLERR | select.POLLHUP
READ_MASK = select.POLLIN | select.POLLPRI
WRITE_MASK = select.POLLOUT
POLLNVAL = select.POLLNVAL


class Hub(HubSkeleton):
    WRITE = WRITE
    READ = READ

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.listeners = [{}, {}]
        self.listeners_r = self.listeners[READ]
        self.listeners_w = self.listeners[WRITE]
        self.secondaries = ({}, {})
        self.closed = []
        self.timers = []

        self.poll = select.epoll()
        #

    def add_timer(self, timer):
        scheduled_time = self.clock() + timer.seconds
        timer.scheduled_time = scheduled_time
        heappush(self.timers, (scheduled_time, timer))
        return timer
        #

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """
        found = False
        for evtype in EVENT_TYPES:
            for listener in self.secondaries[evtype].pop(fileno, []):
                found = True
                self.closed.append(listener)
                listener.defang()

            # For the primary listeners, we actually need to call remove,
            # which may modify the underlying OS polling objects.
            listener = self.listeners[evtype].get(fileno)
            if listener:
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

    def run(self, *a, **kw):
        """Run the runloop until abort is called.
        """
        # accept and discard variable arguments because they will be
        # supplied if other greenlets have run and exited before the
        # hub's greenlet gets a chance to run
        if self.running:
            raise RuntimeError("Already running!")
        try:
            self.running = True
            self.stopping = False

            timers = self.timers

            closed = self.closed

            wait = self.wait
            delay = 0
            clock = self.clock

            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    listener = closed.pop(-1)
                    if not listener.greenlet.dead:
                        # There's no point signalling a greenlet that's already dead.
                        listener.tb(eventlet.hubs.IOClosed(errno.ENOTCONN, "Operation on closed file"))

                when = clock()
                while timers:
                    # current evaluated
                    exp, timer = timers[0]
                    if timer.called:
                        # remove called/cancelled timer
                        heappop(timers)
                        continue
                    due = exp - when
                    if due > 0:
                        break
                    delay = (delay + due) / 2  # delay is negative value

                    # remove evaluated event
                    heappop(timers)
                    try:
                        timer()
                    except SYSTEM_EXCEPTIONS:
                        raise
                    except:
                        pass

                if timers:
                    sleep_time = timers[0][0] - clock() + delay
                    if sleep_time < 0:
                        sleep_time = 0
                else:
                    sleep_time = DEFAULT_SLEEP
                wait(sleep_time)

            else:
                del self.timers[:]
                del self.closed[:]
        finally:
            self.running = False
            self.stopping = False
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

    def wait(self, seconds=0):
        try:
            presult = self.poll.poll(seconds)
        except (IOError, select.error) as e:
            if get_errno(e) == errno.EINTR:
                return
            raise
        except self.SYSTEM_EXCEPTIONS:
            raise

        for fileno, event in presult:
            if event & POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            try:
                if event & EXC_MASK:
                    l = self.listeners_r.get(fileno)
                    if l:
                        l.cb(fileno)
                    l = self.listeners_w.get(fileno)
                    if l:
                        l.cb(fileno)
                    continue
                if event & READ_MASK:
                    l = self.listeners_r.get(fileno)
                    if l:
                        l.cb(fileno)
                if event & WRITE_MASK:
                    l = self.listeners_w.get(fileno)
                    if l:
                        l.cb(fileno)
            except:
                self.squelch_exception(fileno, sys.exc_info())
                clear_sys_exc_info()
        #

    def register(self, fileno, new=False):
        mask = 0
        if fileno in self.listeners_r:
            mask |= READ_MASK | EXC_MASK
        if fileno in self.listeners_w:
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

    def add(self, *args):
        """ *args: evtype, fileno, cb, tb, mac """
        evtype, fileno = args[0:2]
        exists = (fileno in self.listeners_r or fileno in self.listeners_w)

        listener = self.lclass(*args)
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
                        evtype, fileno, evtype, args[2], bucket[fileno]))
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
        for evtype in EVENT_TYPES:
            for l in [self.listeners[evtype].pop(fileno, None)]+self.secondaries[evtype].pop(fileno, []):
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
