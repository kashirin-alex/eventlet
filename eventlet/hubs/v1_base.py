import errno
import sys
import traceback
from collections import deque
import heapq

import eventlet
from eventlet import support
from eventlet.hubs.v1_skeleton import HubSkeleton

# EVENT TYPE INDEX FOR listeners TUPLE
READ = 0
WRITE = 1
event_types = (READ, WRITE)

heappush = heapq.heappush
heappop = heapq.heappop


class HubBase(HubSkeleton):
    """ HubBase class for easing the implementation of subclasses to Timers, Listeners and Listeners Events"""

    READ = READ
    WRITE = WRITE
    event_types = event_types

    def __init__(self, clock=None):
        super(HubBase, self).__init__(clock)

        self.listeners = ({}, {})
        self.listeners_r = self.listeners[READ]
        self.listeners_w = self.listeners[WRITE]
        self.secondaries = ({}, {})
        self.closed = []

        self.timers = []
        self.next_timers = []
        self.add_next_timer = self.next_timers.append
        self.timer_delay = 0

        self.listeners_events = deque()
        self.add_listener_events = self.listeners_events.append
        #

    def has_listeners_fileno(self, fileno):
        return fileno in self.listeners_r or fileno in self.listeners_w
        #

    def has_listener_reader(self, fileno):
        return fileno in self.listeners_r
        #

    def has_listener_writer(self, fileno):
        return fileno in self.listeners_w
        #

    def exist_listeners(self):
        return bool(self.listeners_w) or bool(self.listeners_r)

    def add(self, *args):
        return self.add_listener(*args)
        #

    def add_listener(self, *args):
        """ args: evtype, fileno, cb, tb, mark_as_closed
        Signals an intent to or write a particular file descriptor.
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
        listener = self.lclass(*args)
        evtype, fileno = args[0:2]
        bucket = self.listeners[evtype]
        if fileno in bucket:
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
        return listener
        #

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """

        found = False
        for evtype in event_types:
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

    def notify_close(self, fileno):
        """ We might want to do something when a fileno is closed.
            However, currently it suffices to obsolete listeners only
            when we detect an old fileno being recycled, on open.
        """
        pass

    def remove(self, listener):
        self.remove_listener(listener)
        #

    def remove_listener(self, listener):
        if listener.spent:
            return

        fileno = listener.fileno
        evtype = listener.evtype
        sec = self.secondaries[evtype].get(fileno)
        if not sec:
            self.listeners[evtype].pop(fileno, None)
            return
        # migrate a secondary listener to be the primary listener
        self.listeners[evtype][fileno] = sec.pop(0)
        if not sec:
            self.secondaries[evtype].pop(fileno)
        #

    def mark_as_reopened(self, fileno):
        """ If a file descriptor is returned by the OS as the result of some
            open call (or equivalent), that signals that it might be being
            recycled.

            Catch the case where the fd was previously in use.
        """
        self._obsolete(fileno)

    # Not Implemented
    def remove_descriptor(self, fileno):
        self.remove_descriptor_from_listeners(fileno)
        #

    def remove_descriptor_from_listeners(self, fileno):
        """ Completely remove all listeners for this fileno.  For internal use
        only."""
        for evtype in self.event_types:
            for l in [self.listeners[evtype].pop(fileno, None)]+self.secondaries[evtype].pop(fileno, []):
                if l is None:
                    continue
                self.add_listener_events((evtype, fileno))
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

    def squelch_exception(self, fileno, exc_info):
        traceback.print_exception(*exc_info)
        sys.stderr.write("Removing descriptor: %r\n" % (fileno,))
        sys.stderr.flush()
        try:
            self.remove_descriptor(fileno)
        except Exception as e:
            sys.stderr.write("Exception while removing descriptor! %r\n" % (e,))
            sys.stderr.flush()

    # Not Implemented
    def wait(self, seconds=None):
        raise NotImplementedError("Implement this in a subclass")

    # Not Implemented
    def timer_canceled(self, timer):
        pass

    @staticmethod
    def default_sleep():
        return 60.0

    def add_timer(self, timer):
        timer.scheduled_time = self.clock() + timer.seconds
        self.add_next_timer(timer)
        return timer
        #

    def prepare_timers(self):
        pop = self.next_timers.pop
        while self.next_timers:
            timer = pop(-1)
            if not timer.called:
                heappush(self.timers, (timer.scheduled_time, timer))
        #

    def fire_timers(self, when):
        debug_blocking = self.debug_blocking
        timers = self.timers

        while timers:
            # current evaluated
            exp, t = timers[0]
            if t.called:
                # remove called/cancelled timer
                heappop(timers)
                continue
            due = exp - when  # self.clock()
            if due > 0:
                return
            self.timer_delay += due # delay is negative value
            self.timer_delay /= 2
            # remove evaluated event
            heappop(timers)

            if debug_blocking:
                self.block_detect_pre()
            try:
                t()
            except self.SYSTEM_EXCEPTIONS:
                raise
            except:
                if self.debug_exceptions:
                    self.squelch_generic_exception(sys.exc_info())
                support.clear_sys_exc_info()

            if debug_blocking:
                self.block_detect_post()
        #

    # for debugging:

    def get_readers(self):
        return self.listeners_r.values()

    def get_writers(self):
        return self.listeners_w.values()

    def get_timers_count(self):
        return len(self.timers)+len(self.next_timers)

    def get_listeners_count(self):
        return len(self.listeners_r),  len(self.listeners_w)

    def get_listeners_events_count(self):
        return len(self.listeners_events)
