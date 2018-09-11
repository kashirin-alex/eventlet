import errno
import sys
import traceback
from collections import deque

import eventlet
from eventlet.hubs.v1_skeleton import HubSkeleton

# EVENT TYPE INDEX FOR listeners TUPLE
READ = 0
WRITE = 1
event_types = (READ, WRITE)


class HubBase(HubSkeleton):
    """ HubBase class for easing the implementation of subclasses to Timers, Listeners and Listeners Events"""

    READ = READ
    WRITE = WRITE
    event_types = event_types

    def __init__(self, clock=None):
        super(HubBase, self).__init__(clock)

        self.listeners = ({}, {})
        self.secondaries = ({}, {})
        self.closed = []

        self.timers = []

        self.listeners_events = deque()
        self.add_listener_event = self.listeners_events.append
        #

    def has_listeners_fileno(self, fileno):
        return fileno in self.listeners[READ] or fileno in self.listeners[WRITE]
        #

    def has_listener_reader(self, fileno):
        return fileno in self.listeners[READ]
        #

    def has_listener_writer(self, fileno):
        return fileno in self.listeners[WRITE]
        #

    def exist_listeners(self):
        return bool(self.listeners[WRITE] or self.listeners[READ])

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

    def add_fd_event_read(self, fileno):
        if fileno in self.listeners[READ]:
            self.add_listener_event(self.listeners[READ][fileno])
        #

    def add_fd_event_write(self, fileno):
        if fileno in self.listeners[WRITE]:
            self.add_listener_event(self.listeners[WRITE][fileno])
        #

    def add_fd_event_error(self, fileno):
        self.add_fd_event_read(fileno)
        self.add_fd_event_write(fileno)
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

    def notify_close(self, fileno):
        """ We might want to do something when a fileno is closed.
            However, currently it suffices to obsolete listeners only
            when we detect an old fileno being recycled, on open.
        """
        pass

    def remove(self, listener):
        if listener.spent:
            return
        self.remove_listener(listener)
        #

    def remove_listener(self, listener):
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
        for evtype in event_types:
            l = self.listeners[evtype].pop(fileno)
            if l:
                self.add_listener_event((evtype, fileno, l))
            for l in self.secondaries[evtype].pop(fileno, []):
                self.add_listener_event((evtype, fileno, l))
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

    @staticmethod
    def default_sleep():
        return 60.0

    # for debugging:

    def get_readers(self):
        return self.listeners[READ].values()

    def get_writers(self):
        return self.listeners[WRITE].values()

    def get_timers_count(self):
        return len(self.timers)

    def get_listeners_count(self):
        return len(self.listeners[READ]),  len(self.listeners[WRITE])

    def get_listeners_events_count(self):
        return len(self.listeners_events)
