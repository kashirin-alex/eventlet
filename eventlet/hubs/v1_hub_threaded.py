import errno
import heapq

import sys

from eventlet.support import clear_sys_exc_info

import eventlet
from eventlet.hubs.v1_base import HubBase

orig_threading = eventlet.patcher.original('threading')
ev_sleep = eventlet.patcher.original('time').sleep

heappush = heapq.heappush
heappop = heapq.heappop


class BaseHub(HubBase):
    """ Base hub threaded class for easing the implementation of subclasses that are
    specific to a particular underlying event architecture. """

    DEFAULT_SLEEP = 60.0

    def __init__(self, clock=None):
        super(BaseHub, self).__init__(clock)

        self.event_notifier = orig_threading.Event()
        self.events_waiter = None
        #

    def waiting_thread(self):
        is_set = self.event_notifier.is_set
        notify = self.event_notifier.set
        wait = self.wait
        while not self.stopping:
            wait()
            if not is_set():
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

        wait = self.event_notifier.wait
        wait_clear = self.event_notifier.clear

        timers = self.timers
        fire_timers = self.fire_timers
        prepare_timers = self.prepare_timers

        closed = self.closed
        pop_closed = self.closed.pop
        close_one = self.close_one

        fd_events = self.listeners_events
        pop_fd_event = fd_events.popleft
        listeners = self.listeners

        sys_exec = self.SYSTEM_EXCEPTIONS
        squelch_exception = self.squelch_exception
        try:
            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    close_one(pop_closed(-1))

                prepare_timers()
                fire_timers(self.clock())
                prepare_timers()

                if not fd_events:
                    if timers:
                        due = timers[0][0] - self.clock() + self.timer_delay
                        if due < 0:
                            continue
                    else:
                        due = self.default_sleep()
                    wait(due)  # wait for fd signals
                    wait_clear()

                # Process all fds events
                while fd_events:
                    ev, fileno = pop_fd_event()
                    try:
                        l = listeners[ev].get(fileno)
                        if l:
                            l.cb(fileno)
                    except sys_exec:
                        raise
                    except:
                        squelch_exception(fileno, sys.exc_info())
                        clear_sys_exc_info()

            else:
                del self.timers[:]
                del self.next_timers[:]
                del self.listeners_events[:]
                del self.closed[:]
        finally:
            self.running = False
            self.stopping = False
        #

    def add_listener_event(self, listener):
        self.add_next_timer((self.clock(), listener))
        #
