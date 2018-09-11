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

        loop_ops = self.run_loop_ops
        while not self.stopping:
            # simplify memory de-allocations by method's scope destructor
            try:
                loop_ops()
            except Exception as e:
                print (e, sys.exc_info())

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

        # Process on fd event at a time
        if self.listeners_events:
            print ('yes events', len(self.listeners_events))

        while self.listeners_events:
            # call on fd
            listener = self.listeners_events.popleft()
            if self.debug_blocking:
                self.block_detect_pre()
            try:
                listener.cb(listener.fileno)
            except self.SYSTEM_EXCEPTIONS:
                raise
            except:
                self.squelch_exception(listener.fileno, sys.exc_info())
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
            if not self.listeners_events:
                print ('no events, timers', len(self.listeners_events))
                # wait for fd signals
                self.event_notifier.wait(self.DEFAULT_SLEEP)
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
            if self.next_timers or sleep_time+self.timer_delay > 0:
                print ('next_timers, sleep_time', len(self.next_timers), sleep_time+self.timer_delay)
                ev_sleep(0)
                return
            if not self.next_timers and not self.listeners_events:
                print ('no events, sleep_time', len(self.listeners_events))
                # wait for fd signals
                self.event_notifier.wait(sleep_time)
                self.event_notifier.clear()
            return
        self.timer_delay = (sleep_time + self.timer_delay) / 2  # delay is negative value
        # remove evaluated timer
        heappop(timers)

        # call on timer
        if self.debug_blocking:
            self.block_detect_pre()
        try:
            timer()
        except self.SYSTEM_EXCEPTIONS:
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

    def get_timers_count(self):
        return len(self.timers)+len(self.next_timers)
