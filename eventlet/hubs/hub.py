import errno
import heapq
import os
import sys
import traceback
import signal
from collections import deque

arm_alarm = None
if hasattr(signal, 'setitimer'):
    def alarm_itimer(seconds):
        signal.setitimer(signal.ITIMER_REAL, seconds)
    arm_alarm = alarm_itimer
else:
    try:
        import itimer
        arm_alarm = itimer.alarm
    except ImportError:
        import math

        def alarm_signal(seconds):
            signal.alarm(math.ceil(seconds))
        arm_alarm = alarm_signal

import eventlet
from eventlet.support import greenlets as greenlet, clear_sys_exc_info

if os.environ.get('EVENTLET_CLOCK'):
    mod = os.environ.get('EVENTLET_CLOCK').rsplit('.', 1)
    default_clock = getattr(eventlet.patcher.original(mod[0]), mod[1])
    del mod
else:
    import monotonic
    default_clock = monotonic.monotonic
    del monotonic

heappush = heapq.heappush
heappop = heapq.heappop

g_prevent_multiple_readers = True

READ = "read"
WRITE = "write"


def closed_callback(fileno):
    """ Used to de-fang a callback that may be triggered by a loop in BaseHub.wait
    """
    # No-op.
    pass


class FdListener(object):
    __slots__ = ['evtype', 'fileno', 'cb', 'tb', 'mark_as_closed', 'spent', 'greenlet']

    def __init__(self, *args):
        """ The following are required:
        *args : evtype, fileno, cb, tb, mark_as_closed
        cb - the standard callback, which will switch into the
            listening greenlet to indicate that the event waited upon
            is ready
        tb - a 'throwback'. This is typically greenlet.throw, used
            to raise a signal into the target greenlet indicating that
            an event was obsoleted by its underlying filehandle being
            repurposed.
        mark_as_closed - if any listener is obsoleted, this is called
            (in the context of some other client greenlet) to alert
            underlying filehandle-wrapping objects that they've been
            closed.
        """
        self.evtype, self.fileno, self.cb, self.tb, self.mark_as_closed = args
        self.spent = False
        self.greenlet = eventlet.getcurrent()

    def __repr__(self):
        return "%s(%r, %r, %r, %r)" % (type(self).__name__, self.evtype, self.fileno,
                                       self.cb, self.tb)
    __str__ = __repr__

    def defang(self):
        self.cb = closed_callback
        if self.mark_as_closed is not None:
            self.mark_as_closed()
        self.spent = True


class FdListeners:

    def __init__(self, *ev_types):
        self.ev_types = set(ev_types)
        for ev_type in ev_types:
            setattr(self, ev_type, {})

    def __getitem__(self, ev_type):
        return getattr(self, ev_type)

    def __setitem__(self, ev_type):
        if ev_type not in self.ev_types:
            self.ev_types.add(ev_type)
        setattr(self, ev_type, {})

    def has_fileno(self, fileno):
        for ev_type in self.ev_types:
            if fileno in getattr(self, ev_type):
                return True
        return False

noop = FdListener(READ, 0, lambda x: None, lambda x: None, None)


# in debug mode, track the call site that created the listener


class DebugListener(FdListener):
    __slots__ = FdListener.__slots__ + ['where_called']

    def __init__(self, evtype, fileno, cb, tb, mark_as_closed):
        super(DebugListener, self).__init__(evtype, fileno, cb, tb, mark_as_closed)
        self.where_called = traceback.format_stack()

    def __repr__(self):
        return "DebugListener(%r, %r, %r, %r, %r, %r)\n%sEndDebugFdListener" % (
            self.evtype,
            self.fileno,
            self.cb,
            self.tb,
            self.mark_as_closed,
            self.greenlet,
            ''.join(self.where_called))
    __str__ = __repr__


def alarm_handler(signum, frame):
    import inspect
    raise RuntimeError("Blocking detector ALARMED at" + str(inspect.getframeinfo(frame)))

SYSTEM_EXCEPTIONS = (KeyboardInterrupt, SystemExit)


class BaseHub(object):
    """ Base hub class for easing the implementation of subclasses that are
    specific to a particular underlying event architecture. """

    SYSTEM_EXCEPTIONS = SYSTEM_EXCEPTIONS

    READ = READ
    WRITE = WRITE

    def __init__(self, clock=None):
        self.listeners = FdListeners(self.READ, self.WRITE)
        self.secondaries = FdListeners(self.READ, self.WRITE)
        self.closed = []
        self.lclass = FdListener
        self.listeners_events = deque()

        self.clock = default_clock if clock is None else clock
        self.timers = []
        self.next_timers = []

        self.greenlet = greenlet.greenlet(self.run)
        self.stopping = False
        self.running = False

        self.debug_exceptions = False
        self.debug_blocking = False
        self.debug_blocking_resolution = 1
        self._old_signal_handler = None

    def block_detect_pre(self):
        # shortest alarm we can possibly raise is one second
        tmp = signal.signal(signal.SIGALRM, alarm_handler)
        if tmp != alarm_handler:
            self._old_signal_handler = tmp

        arm_alarm(self.debug_blocking_resolution)

    def block_detect_post(self):
        if (hasattr(self, "_old_signal_handler") and
                self._old_signal_handler):
            signal.signal(signal.SIGALRM, self._old_signal_handler)
        signal.alarm(0)

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
        listener = self.lclass(evtype, fileno, cb, tb, mark_as_closed)
        bucket = self.listeners[evtype]
        if fileno in bucket:
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
        return listener

    def _obsolete(self, fileno):
        """ We've received an indication that 'fileno' has been obsoleted.
            Any current listeners must be defanged, and notifications to
            their greenlets queued up to send.
        """

        found = False
        for evtype in [WRITE, READ]:
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
            # trampoline may trigger this in its finally section.
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
            del self.secondaries[evtype][fileno]

    def mark_as_reopened(self, fileno):
        """ If a file descriptor is returned by the OS as the result of some
            open call (or equivalent), that signals that it might be being
            recycled.

            Catch the case where the fd was previously in use.
        """
        self._obsolete(fileno)

    def remove_descriptor(self, fileno):
        """ Completely remove all listeners for this fileno.  For internal use
        only."""
        listeners = []
        for evtype in [WRITE, READ]:
            l = self.listeners[evtype].get(fileno)
            if l:
                listeners.append(l)
            listeners += self.secondaries[evtype].get(fileno, [])

        for listener in listeners:
            self._listener_callback(listener)

    @staticmethod
    def close_one(listener):
        """ Triggered from the main run loop. If a listener's underlying FD was
            closed somehow, throw an exception back to the trampoline, which should
            be able to manage it appropriately.
        """
        if not listener.greenlet.dead:
            # There's no point signalling a greenlet that's already dead.
            listener.tb(eventlet.hubs.IOClosed(errno.ENOTCONN, "Operation on closed file"))

    def ensure_greenlet(self):
        if not self.greenlet.dead:
            return
        # create new greenlet sharing same parent as original
        new = greenlet.greenlet(self.run, self.greenlet.parent)
        # need to assign as parent of old greenlet
        # for those greenlets that are currently
        # children of the dead hub and may subsequently
        # exit without further switching to hub.
        self.greenlet.parent = new
        self.greenlet = new

    def switch(self):
        cur = eventlet.getcurrent()
        assert cur is not self.greenlet, 'Cannot switch to MAINLOOP from MAINLOOP'
        switch_out = getattr(cur, 'switch_out', None)
        if switch_out is not None:
            try:
                switch_out()
            except:
                if self.debug_exceptions:
                    self.squelch_generic_exception(sys.exc_info())
        self.ensure_greenlet()
        try:
            if self.greenlet.parent is not cur:
                cur.parent = self.greenlet
        except ValueError:
            pass  # gets raised if there is a greenlet parent cycle
        clear_sys_exc_info()
        return self.greenlet.switch()

    def squelch_exception(self, fileno, exc_info):
        traceback.print_exception(*exc_info)
        sys.stderr.write("Removing descriptor: %r\n" % (fileno,))
        sys.stderr.flush()
        try:
            self.remove_descriptor(fileno)
        except Exception as e:
            sys.stderr.write("Exception while removing descriptor! %r\n" % (e,))
            sys.stderr.flush()

    def wait(self, seconds=None):
        raise NotImplementedError("Implement this in a subclass")

    @staticmethod
    def default_sleep():
        return 60.0

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

            writers = self.listeners[WRITE]
            readers = self.listeners[READ]
            closed = self.closed
            listeners_events = self.listeners_events
            process_listener_events = self.process_listener_events
            timers = self.timers
            next_timers = self.next_timers

            wait = self.wait
            close_one = self.close_one

            delay = 0

            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    close_one(closed.pop(-1))

                # Process one fd event at a time
                if listeners_events:
                    process_listener_events(listeners_events.popleft())

                # Assign new timers
                while next_timers:
                    timer = next_timers.pop(-1)
                    if not timer.called:
                        heappush(timers, (timer.scheduled_time, timer))

                if not timers:
                    if not listeners_events:
                        # wait for fd signals
                        wait(self.default_sleep())
                    continue

                # current evaluated timer
                exp, timer = timers[0]
                if timer.called:
                    # remove called/cancelled timer
                    heappop(timers)
                    continue

                sleep_time = exp - self.clock()
                if sleep_time > 0:
                    if not listeners_events:
                        # wait for fd signals
                        sleep_time += delay
                        wait(sleep_time if sleep_time > 0 else 0)
                    continue
                delay = (sleep_time+delay)/2  # delay is negative value

                # remove current evaluated timer
                heappop(timers)

                # check for fds new signals
                if not listeners_events and (readers or writers):
                    wait(0)

                if self.debug_blocking:
                    self.block_detect_pre()
                try:
                    timer()
                except SYSTEM_EXCEPTIONS:
                    raise
                except:
                    if self.debug_exceptions:
                        self.squelch_timer_exception(timer, sys.exc_info())
                    clear_sys_exc_info()
                if self.debug_blocking:
                    self.block_detect_post()

            else:
                del self.timers[:]
        finally:
            self.running = False
            self.stopping = False
        #

    def process_listener_events(self, *args):
        if self.debug_blocking:
            self.block_detect_pre()

        ev_type, file_no = args
        try:
            if ev_type is not None:
                listener = self.listeners[ev_type].get(file_no)
                if listener is not None:
                    listener.cb(file_no)
            else:
                listener = self.listeners[self.READ].get(file_no)
                if listener is not None:
                    listener.cb(file_no)
                listener = self.listeners[self.WRITE].get(file_no)
                if listener is not None:
                    listener.cb(file_no)
        except SYSTEM_EXCEPTIONS:
            raise
        except:
            self.squelch_exception(file_no, sys.exc_info())
            clear_sys_exc_info()

        if self.debug_blocking:
            self.block_detect_post()
        #

    @staticmethod
    def fire_timers(when):
        # intermediate dummy place-holder
        return

    @staticmethod
    def prepare_timers():
        # intermediate dummy place-holder
        return

    def abort(self, wait=False):
        """Stop the runloop. If run is executing, it will exit after
        completing the next runloop iteration.

        Set *wait* to True to cause abort to switch to the hub immediately and
        wait until it's finished processing.  Waiting for the hub will only
        work from the main greenthread; all other greenthreads will become
        unreachable.
        """
        if self.running:
            self.stopping = True
        if wait:
            assert self.greenlet is not eventlet.getcurrent(
            ), "Can't abort with wait from inside the hub's greenlet."
            # schedule an immediate timer just so the hub doesn't sleep
            self.schedule_call_global(0, lambda: None)
            # switch to it; when done the hub will switch back to its parent,
            # the main greenlet
            self.switch()

    def squelch_generic_exception(self, exc_info):
        if self.debug_exceptions:
            traceback.print_exception(*exc_info)
            sys.stderr.flush()
            clear_sys_exc_info()

    def squelch_timer_exception(self, timer, exc_info):
        if self.debug_exceptions:
            traceback.print_exception(*exc_info)
            sys.stderr.flush()
            clear_sys_exc_info()

    def add_timer(self, timer):
        timer.scheduled_time = self.clock() + timer.seconds
        self.next_timers.append(timer)
        return timer

    def schedule_call_local(self, seconds, cb, *args, **kw):
        """Schedule a callable to be called after 'seconds' seconds have
        elapsed. Cancel the timer if greenlet has exited.
            seconds: The number of seconds to wait.
            cb: The callable to call after the given time.
            *args: Arguments to pass to the callable when called.
            **kw: Keyword arguments to pass to the callable when called.
        """
        return self.add_timer(eventlet.LocalTimer(seconds, cb, *args, **kw))

    def schedule_call_global(self, seconds, cb, *args, **kw):
        """Schedule a callable to be called after 'seconds' seconds have
        elapsed. The timer will NOT be canceled if the current greenlet has
        exited before the timer fires.
            seconds: The number of seconds to wait.
            cb: The callable to call after the given time.
            *args: Arguments to pass to the callable when called.
            **kw: Keyword arguments to pass to the callable when called.
        """
        return self.add_timer(eventlet.Timer(seconds, cb, *args, **kw))

    # for debugging:

    def get_readers(self):
        return self.listeners[READ].values()

    def get_writers(self):
        return self.listeners[WRITE].values()

    def get_timers_count(self):
        return self.timers.__len__()+self.next_timers.__len__()

    def set_debug_listeners(self, value):
        self.lclass = DebugListener if value else FdListener

    def set_timer_exceptions(self, value):
        self.debug_exceptions = value
