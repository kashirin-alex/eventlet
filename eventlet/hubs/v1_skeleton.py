import os
import traceback
import signal
import eventlet
import sys
from eventlet import support

if os.environ.get('EVENTLET_CLOCK'):
    mod = os.environ.get('EVENTLET_CLOCK').rsplit('.', 1)
    default_clock = getattr(eventlet.patcher.original(mod[0]), mod[1])
    del mod
else:
    import monotonic
    default_clock = monotonic.monotonic
    del monotonic


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


def closed_callback(fileno):
    """ Used to de-fang a callback that may be triggered by a loop in BaseHub.wait
    """
    # No-op.
    pass


def alarm_handler(signum, frame):
    import inspect
    raise RuntimeError("Blocking detector ALARMED at" + str(inspect.getframeinfo(frame)))


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
        #

    def __call__(self):
        self.cb(self.fileno)
        #

    def __repr__(self):
        return "%s(%r, %r, %r, %r, %r)" % (type(self).__name__, self.evtype, self.fileno, self.spent, self.cb, self.tb)
    __str__ = __repr__

    def defang(self):
        self.cb = closed_callback
        if self.mark_as_closed is not None:
            self.mark_as_closed()
        self.spent = True


class HubFileDetails(object):
    """ HubFileDetails class for keeping a fileno listeners - readers+writers"""

    __slots__ = ['rs', 'ws']

    def __init__(self, listener, a_reader):
        self.rs = [listener] if a_reader else []
        self.ws = [listener] if not a_reader else []
        #

    def add(self, listener, a_reader, prevent_multiple):
        l = self.rs if a_reader else self.ws

        if l and prevent_multiple:
            raise RuntimeError(
                "Second simultaneous %s on fileno %s "
                "detected.  Unless you really know what you're doing, "
                "make sure that only one greenthread can %s any "
                "particular socket.  Consider using a pools.Pool. "
                "If you do know what you're doing and want to disable "
                "this error, call "
                "eventlet.debug.hub_prevent_multiple_readers(False) - MY THREAD=%s; "
                "THAT THREAD=%s" % (listener.evtype, listener.fileno, listener.evtype, listener.cb, listener))
        l.append(listener)
        #

    def remove(self, listener, a_reader):
        (self.rs if a_reader else self.ws).remove(listener)
        #

    def clear(self):
        del self.rs[:]
        del self.ws[:]
        #

    def __nonzero__(self):
        return bool(self.rs) or bool(self.ws)
        #
    __bool__ = __nonzero__

    def __len__(self):
        return len(self.rs)+len(self.ws)
        #

    def __iter__(self):
        for l in self.rs:
            yield l
        for l in self.ws:
            yield l
        #


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


class HubSkeleton(object):
    """ HubSkeleton class for easing the implementation of subclasses to greenlet, debug and Listener"""

    SYSTEM_EXCEPTIONS = (KeyboardInterrupt, SystemExit)

    def __init__(self, clock=None):
        self.clock = default_clock if clock is None else clock
        self.lclass = FdListener

        self.greenlet = support.greenlets.greenlet(self.run)
        self.greenlet_switch = self.greenlet.switch
        self.stopping = False
        self.running = False

        self.debug_exceptions = False
        self.debug_blocking = False
        self.debug_blocking_resolution = 1
        self._old_signal_handler = None
        self.g_prevent_multiple_readers = True
        #

    # Not Implemented
    def run(self, *a, **kw):
        raise NotImplementedError("Implement this in a subclass")
        #

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
        support.clear_sys_exc_info()
        return self.greenlet_switch()
        #

    def ensure_greenlet(self):
        if not self.greenlet.dead:
            return
        # create new greenlet sharing same parent as original
        #
        # need to assign as parent of old greenlet
        # for those greenlets that are currently
        # children of the dead hub and may subsequently
        # exit without further switching to hub.
        # - waiting_thread will continue to add fd events to the listeners_events
        # or start new Thread depends on the state of self.events_waiter with the new greenlet
        self.greenlet = self.greenlet.parent = support.greenlets.greenlet(self.run, self.greenlet.parent)
        self.greenlet_switch = self.greenlet.switch
        #

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
        #

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

    def block_detect_pre(self):
        # shortest alarm we can possibly raise is one second
        tmp = signal.signal(signal.SIGALRM, alarm_handler)
        if tmp != alarm_handler:
            self._old_signal_handler = tmp

        arm_alarm(self.debug_blocking_resolution)
        #

    def block_detect_post(self):
        if (hasattr(self, "_old_signal_handler") and
                self._old_signal_handler):
            signal.signal(signal.SIGALRM, self._old_signal_handler)
        signal.alarm(0)
        #

    def set_debug_listeners(self, value):
        self.lclass = DebugListener if value else FdListener

    def set_timer_exceptions(self, value):
        self.debug_exceptions = value

    def squelch_generic_exception(self, exc_info):
        if self.debug_exceptions:
            traceback.print_exception(*exc_info)
            sys.stderr.flush()
            support.clear_sys_exc_info()
        #

    # Not Implemented
    def add_timer(self, timer):
        raise NotImplementedError("Implement this in a subclass")
        #
