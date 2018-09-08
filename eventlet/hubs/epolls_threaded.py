import errno
import heapq
import sys
import traceback
import signal
from collections import deque

import eventlet
from eventlet.support import greenlets as greenlet, clear_sys_exc_info
from eventlet import support
from eventlet.hubs.common import (FdListener, DebugListener,
                                  alarm_handler, default_clock, arm_alarm, SYSTEM_EXCEPTIONS)

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


class Hub(object):
    """ epolls Hub with Threaded Poll Waiter. """

    READ = READ
    WRITE = WRITE

    def __init__(self, clock=None):
        self.listeners = ({}, {})
        self.listeners_r = self.listeners[READ]
        self.listeners_w = self.listeners[WRITE]

        self.secondaries = ({}, {})
        self.closed = []
        self.lclass = FdListener

        self.clock = default_clock if clock is None else clock

        self.timers = []
        self.next_timers = []
        self.listeners_events = deque()
        self.event_notifier = orig_threading.Event()
        self.events_waiter = None

        self.greenlet = greenlet.greenlet(self.run)
        self.stopping = False
        self.running = False

        self.debug_exceptions = False
        self.debug_blocking = False
        self.debug_blocking_resolution = 1
        self._old_signal_handler = None

        self.poll = select.epoll()
        self.poll_register = self.poll.register
        self.poll_modify = self.poll.modify
        self.poll_unregister = self.poll.unregister
        #

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

        new = not (fileno in self.listeners_r or fileno in self.listeners_w)

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
        if fileno in self.listeners_r:
            mask |= READ_MASK | EXC_MASK
        if fileno in self.listeners_w:
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
        listeners = []
        for evtype in event_types:
            l = self.listeners[evtype].get(fileno)
            if l:
                listeners.append(l)
            listeners += self.secondaries[evtype].get(fileno, [])

        for listener in listeners:
            self.add_listener_event(listener.evtype, listener.fileno)

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

    def ensure_greenlet(self):
        if not self.greenlet.dead:
            return
        # create new greenlet sharing same parent as original
        new = greenlet.greenlet(self.run, self.greenlet.parent)
        # need to assign as parent of old greenlet
        # for those greenlets that are currently
        # children of the dead hub and may subsequently
        # exit without further switching to hub.
        # - waiting_thread will continue to add fd events to the listeners_events
        # or start new Thread depends on the state of self.events_waiter with the new greenlet
        self.greenlet.parent = new
        self.greenlet = new
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
        clear_sys_exc_info()
        return self.greenlet.switch()
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

    def waiting_thread(self):
        poll = self.poll.poll
        add_events = self.listeners_events.append

        no_waiters = self.event_notifier.is_set
        notify = self.event_notifier.set

        while not self.stopping:
            presult = None
            try:
                presult = poll(DEFAULT_SLEEP)
            except SYSTEM_EXCEPTIONS:
                raise

            if not presult:
                ev_sleep(3)
                continue

            for fileno, event in presult:
                if event & POLLNVAL:
                    self.remove_descriptor(fileno)
                    continue
                if event & EXC_MASK:
                    add_events((None, fileno))
                    continue
                if event & READ_MASK:
                    add_events((READ, fileno))
                if event & WRITE_MASK:
                    add_events((WRITE, fileno))

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
        try:
            self.running = True
            self.stopping = False

            clock = self.clock
            delay = 0

            timers = self.timers
            next_timers = self.next_timers
            pop_next_timer = self.next_timers.pop

            listeners = self.listeners
            get_writer = listeners[WRITE].get
            get_reader = listeners[READ].get
            listeners_events = self.listeners_events
            listener_event_next = self.listeners_events.popleft
            closed = self.closed
            pop_closed = self.closed.pop
            close_one = self.close_one

            if self.events_waiter is None or not self.events_waiter.is_alive():
                self.events_waiter = orig_threading.Thread(target=self.waiting_thread)
                self.events_waiter.start()

            self.event_notifier.set()
            event_notifier_wait = self.event_notifier.wait
            event_notifier_clear = self.event_notifier.clear

            while not self.stopping:
                debug_blocking = self.debug_blocking

                # Ditch all closed fds first.
                while closed:
                    close_one(pop_closed(-1))

                # Process all fds events
                while listeners_events:
                    # call on fd cb
                    evtype, fileno = listener_event_next()
                    if debug_blocking:
                        self.block_detect_pre()
                    try:
                        if evtype is READ:
                            l = get_reader(fileno)
                            if l is not None:
                                l.cb(fileno)
                            continue
                        if evtype is WRITE:
                            l = get_writer(fileno)
                            if l is not None:
                                l.cb(fileno)
                            continue

                        l = get_writer(fileno)
                        if l is not None:
                            l.cb(fileno)
                        l = get_reader(fileno)
                        if l is not None:
                            l.cb(fileno)
                    except SYSTEM_EXCEPTIONS:
                        raise
                    except:
                        self.squelch_exception(fileno, sys.exc_info())
                        clear_sys_exc_info()
                    if debug_blocking:
                        self.block_detect_post()

                # Assign new timers
                while next_timers:
                    timer = pop_next_timer(-1)
                    if not timer.called:
                        heappush(timers, (timer.scheduled_time, timer))

                if not timers:
                    ev_sleep(0)
                    if not listeners_events:
                        # wait for fd signals
                        event_notifier_wait(DEFAULT_SLEEP)
                        event_notifier_clear()
                    continue

                # current evaluated timer
                exp, timer = timers[0]
                if timer.called:
                    # remove called/cancelled timer
                    heappop(timers)
                    continue

                sleep_time = exp - clock()
                if sleep_time > 0:
                    sleep_time += delay
                    if sleep_time <= 0 or next_timers:
                        delay = 0  # preserving delay can cause a close loop on a long delay
                        ev_sleep(0)
                        continue
                    if not listeners_events:
                        # wait for fd signals
                        event_notifier_wait(sleep_time)
                        event_notifier_clear()
                    else:
                        ev_sleep(0)
                    continue
                delay = (sleep_time+delay)/2  # delay is negative value

                # remove evaluated timer
                heappop(timers)

                # call on timer
                if debug_blocking:
                    self.block_detect_pre()
                try:
                    timer()
                except SYSTEM_EXCEPTIONS:
                    raise
                except:
                    if self.debug_exceptions:
                        self.squelch_timer_exception(timer, sys.exc_info())
                    clear_sys_exc_info()
                if debug_blocking:
                    self.block_detect_post()

            else:
                del self.timers[:]
                del self.next_timers[:]
                del self.listeners_events[:]
        finally:
            self.running = False
            self.stopping = False
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

    def add_listener_event(self, *evtype_fileno):
        self.listeners_events.append(evtype_fileno)

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
        return self.listeners_r.values()

    def get_writers(self):
        return self.listeners_w.values()

    def get_timers_count(self):
        return self.timers.__len__()+self.next_timers.__len__()

    def get_listeners_count(self):
        return self.listeners_r.__len__(),  self.listeners_w.__len__()

    def get_listeners_events_count(self):
        return self.listeners_events.__len__()

    def set_debug_listeners(self, value):
        self.lclass = DebugListener if value else FdListener

    def set_timer_exceptions(self, value):
        self.debug_exceptions = value
