import os
import traceback
import signal
import eventlet


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


SYSTEM_EXCEPTIONS = (KeyboardInterrupt, SystemExit)


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

    def __repr__(self):
        return "%s(%r, %r, %r, %r)" % (type(self).__name__, self.evtype, self.fileno,
                                       self.cb, self.tb)
    __str__ = __repr__

    def defang(self):
        self.cb = closed_callback
        if self.mark_as_closed is not None:
            self.mark_as_closed()
        self.spent = True


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
