import heapq
import sys

import eventlet
from eventlet import support
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

        self.next_timers = []
        self.add_next_timer = self.next_timers.append

        self.event_notifier = orig_threading.Event()
        self.events_waiter = None
        self.timer_delay = 0
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
        listeners_events = self.listeners_events
        listeners_events_popleft = self.listeners_events.popleft

        closed = self.closed
        closed_pop = self.closed.pop
        close_one = self.close_one

        timers = self.timers
        prepare_timers = self.prepare_timers
        fire_timers = self.fire_timers


        try:
            while not self.stopping:
                debug_blocking = self.debug_blocking

                # Ditch all closed fds first.
                while closed:
                    close_one(closed_pop(-1))

                prepare_timers()
                fire_timers(self.clock())
                prepare_timers()

                if timers:
                    sleep_time = timers[0][0]-self.clock()+self.timer_delay
                    if sleep_time < 0:
                        sleep_time = 0
                else:
                    sleep_time = self.default_sleep()

                wait(sleep_time)
                wait_clear()

                # Process all fds events
                while listeners_events:
                    print ('events', len(listeners_events))
                    listener = listeners_events_popleft()
                    if debug_blocking:
                        self.block_detect_pre()
                    try:
                        listener.cb(listener.fileno)
                    except self.SYSTEM_EXCEPTIONS:
                        raise
                    except:
                        self.squelch_exception(listener.fileno, sys.exc_info())
                        support.clear_sys_exc_info()
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

    def prepare_timers(self):
        # Assign new timers
        while self.next_timers:
            timer = self.next_timers.pop(-1)
            if not timer.called:
                heappush(self.timers, (timer.scheduled_time, timer))
        #

    def fire_timers(self, when):
        debug_blocking = self.debug_blocking
        timers = self.timers

        while timers:
            # current evaluated
            exp, timer = timers[0]
            if timer.called:
                # remove called/cancelled timer
                heappop(timers)
                continue
            due = exp - when  # self.clock()
            if due > 0:
                return
            self.timer_delay += due  # delay is negative value
            self.timer_delay /= 2

            # remove evaluated event
            heappop(timers)

            if debug_blocking:
                self.block_detect_pre()
            try:
                timer()
            except self.SYSTEM_EXCEPTIONS:
                raise
            except:
                if self.debug_exceptions:
                    self.squelch_generic_exception(sys.exc_info())
                support.clear_sys_exc_info()

            if debug_blocking:
                self.block_detect_post()
        #

    def add_timer(self, timer):
        timer.scheduled_time = self.clock() + timer.seconds
        self.add_next_timer(timer)
        return timer

    def get_timers_count(self):
        return len(self.timers)+len(self.next_timers)
