
import errno
import heapq
import sys
import traceback
from collections import deque

import eventlet
from eventlet import support
from eventlet.hubs.common import (HubSkeletonV1, SYSTEM_EXCEPTIONS)

select = eventlet.patcher.original('select')
orig_threading = eventlet.patcher.original('threading')
ev_sleep = eventlet.patcher.original('time').sleep


def is_available():
    return hasattr(select, 'epoll')

g_prevent_multiple_readers = True
DEFAULT_SLEEP = 60.0

# EVENT TYPE INDEX FOR listeners in a TUPLE
READ = 0
WRITE = 1
event_types = (READ, WRITE)

EXC_MASK = select.POLLERR | select.POLLHUP
READ_MASK = select.POLLIN | select.POLLPRI
WRITE_MASK = select.POLLOUT
POLLNVAL = select.POLLNVAL
SELECT_ERR = select.error

heappush = heapq.heappush
heappop = heapq.heappop


class Hub(HubSkeletonV1):
    """ epolls Hub with Threaded Poll Waiter. """

    READ = READ
    WRITE = WRITE

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)

        self.listeners = ({}, {})
        self.secondaries = ({}, {})
        self.closed = []

        self.timers = []
        self.next_timers = []
        self.add_next_timer = self.next_timers.append

        self.listeners_events = deque()
        self.add_listener_event = self.listeners_events.append
        self.event_notifier = orig_threading.Event()
        self.events_waiter = None

        self.poll = select.epoll()
        self.do_poll = self.poll.poll
        self.poll_register = self.poll.register
        self.poll_modify = self.poll.modify
        self.poll_unregister = self.poll.unregister
        #

    def add(self, evtype, fileno, cb, tb, mark_as_closed):
        """ Signals an intent to or write a particular file descriptor.
        The *evtype* argument is either the constant READ or WRITE.
        The *fileno* argument is the file number of the file of interest.
        The *cb* argument is the callback which will be called when the file
        is ready for reading/writing.
        The *tb* argument is the throwback used to signal (into the greenlet)
        that the file was closed.
        The *mark_as_closed* is used in the context of the event hub to
        prepare a Python object as being closed, pre-empting further
        close operations from accidentally shutting down the wrong OS thread.
        """

        new = not (fileno in self.listeners[READ] or fileno in self.listeners[WRITE])

        listener = self.lclass(evtype, fileno, cb, tb, mark_as_closed)
        bucket = self.listeners[evtype]
        if not new and fileno in bucket:
            if g_prevent_multiple_readers:
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
            # new=True, Means we've added a new listener
            self.register(fileno, new=new)
        except IOError as ex:    # ignore EEXIST, #80
            if support.get_errno(ex) != errno.EEXIST:
                raise
        return listener
        #

    def register(self, fileno, new=False):
        mask = 0
        if fileno in self.listeners[READ]:
            mask |= READ_MASK | EXC_MASK
        if fileno in self.listeners[WRITE]:
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

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """
        found = False
        for evtype in event_types:
            bucket = self.secondaries[evtype]
            if fileno in bucket:
                for listener in bucket.pop(fileno):
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

    def notify_close(self, fileno):
        """ We might want to do something when a fileno is closed.
            However, currently it suffices to obsolete listeners only
            when we detect an old fileno being recycled, on open.
        """
        pass

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

    def mark_as_reopened(self, fileno):
        """ If a file descriptor is returned by the OS as the result of some
            open call (or equivalent), that signals that it might be being
            recycled.
            Catch the case where the fd was previously in use.
        """
        self._obsolete(fileno)
        #

    def remove_descriptor(self, fileno):
        """ Completely remove all listeners for this fileno.  For internal use
        only."""
        for evtype in event_types:
            l = self.listeners[evtype].get(fileno)
            if l:
                self.add_listener_event((l.evtype, l.fileno))
            for l in self.secondaries[evtype].get(fileno, []):
                self.add_listener_event((l.evtype, l.fileno))
        try:
            self.poll_unregister(fileno)
        except (KeyError, ValueError, IOError, OSError):
            # raised if we try to remove a fileno that was
            # already removed/invalid
            pass
        #

    @staticmethod
    def close_one(listener):
        """ Triggered from the main run loop. If a listener's underlying FD was
            closed somehow, throw an exception back to the trampoline, which should
            be able to manage it appropriately.
        """
        if not listener.greenlet.dead:
            # There's no point signalling a greenlet that's already dead.
            listener.tb(eventlet.hubs.IOClosed(errno.ENOTCONN, "Operation on closed file"))
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

    def wait(self):
        try:
            presult = self.do_poll(DEFAULT_SLEEP)
        except SYSTEM_EXCEPTIONS:
            raise
        except:
            presult = None

        if not presult:
            ev_sleep(3)
            return

        for fileno, event in presult:
            if event & POLLNVAL:
                self.remove_descriptor(fileno)
                continue
            if event & EXC_MASK:
                self.add_listener_event((None, fileno))
                continue
            if event & READ_MASK:
                self.add_listener_event((READ, fileno))
            if event & WRITE_MASK:
                self.add_listener_event((WRITE, fileno))
        #

    def waiting_thread(self):
        no_waiters = self.event_notifier.is_set
        notify = self.event_notifier.set
        wait = self.wait
        while not self.stopping:
            wait()
            if not no_waiters():
                notify()
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

        if self.events_waiter is None or not self.events_waiter.is_alive():
            self.events_waiter = orig_threading.Thread(target=self.waiting_thread)
            self.events_waiter.start()
        self.event_notifier.set()

        loop_ops = self.run_loop_ops
        while not self.stopping:
            # simplify memory de-allocations by method's scope destructor
            loop_ops()

        del self.timers[:]
        del self.next_timers[:]
        del self.listeners_events[:]

        self.running = False
        self.stopping = False
        #

    def run_loop_ops(self):
        # Ditch all closed fds first.
        while self.closed:
            self.close_one(self.closed.pop(-1))

        # Process all fds events
        while self.listeners_events:
            # call on fd
            evtype, fileno = self.listeners_events.popleft()
            if self.debug_blocking:
                self.block_detect_pre()
            try:
                if evtype is not None:
                    l = self.listeners[evtype].get(fileno)
                    if l is not None:
                        l.cb(fileno)
                else:
                    l = self.listeners[WRITE].get(fileno)
                    if l is not None:
                        l.cb(fileno)
                    l = self.listeners[READ].get(fileno)
                    if l is not None:
                        l.cb(fileno)
            except SYSTEM_EXCEPTIONS:
                raise
            except:
                self.squelch_exception(fileno, sys.exc_info())
                support.clear_sys_exc_info()
            if self.debug_blocking:
                self.block_detect_post()

        timers = self.timers
        # Assign new timers
        while self.next_timers:
            timer = self.next_timers.pop(-1)
            if not timer.called:
                heappush(timers, (timer.scheduled_time, timer))
        if not timers:
            ev_sleep(0)
            if not self.listeners_events:
                # wait for fd signals
                self.event_notifier.wait(DEFAULT_SLEEP)
                self.event_notifier.clear()
            return

        # current evaluated timer
        exp, timer = timers[0]
        if timer.called:
            # remove called/cancelled timer
            heappop(timers)
            return
        sleep_time = exp - self.clock()
        if sleep_time > 0:
            if self.next_timers:
                ev_sleep(0)
                return
            # sleep_time += delay
            # if sleep_time <= 0:
            #    delay = 0  # preserving delay can cause a close loop on a long delay
            #    ev_sleep(0)
            #    return
            if not self.listeners_events:
                # wait for fd signals
                self.event_notifier.wait(sleep_time)
                self.event_notifier.clear()
            else:
                ev_sleep(0)
            return
        # delay = (sleep_time + delay) / 2  # delay is negative value
        # remove evaluated timer
        heappop(timers)

        # call on timer
        if self.debug_blocking:
            self.block_detect_pre()
        try:
            timer()
        except SYSTEM_EXCEPTIONS:
            raise
        except:
            if self.debug_exceptions:
                self.squelch_generic_exception(sys.exc_info())
            support.clear_sys_exc_info()
        if self.debug_blocking:
            self.block_detect_post()
        #

    def add_timer(self, timer):
        timer.scheduled_time = self.clock() + timer.seconds
        self.add_next_timer(timer)
        return timer

    # for debugging:

    def get_readers(self):
        return self.listeners[READ].values()

    def get_writers(self):
        return self.listeners[WRITE].values()

    def get_timers_count(self):
        return len(self.timers)+len(self.next_timers)

    def get_listeners_count(self):
        return len(self.listeners[READ]),  len(self.listeners[WRITE])

    def get_listeners_events_count(self):
        return len(self.listeners_events)
