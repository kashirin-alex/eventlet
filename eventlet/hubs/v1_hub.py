import sys
import heapq

from eventlet import support
from eventlet.hubs.v1_base import HubBase

heappush = heapq.heappush
heappop = heapq.heappop


class BaseHub(HubBase):
    """ Base hub class for easing the implementation of subclasses that are
    specific to a particular underlying event architecture. """

    def __init__(self, clock=None):
        super(BaseHub, self).__init__(clock)
        self.timer_delay = 0
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

            fire_timers = self.fire_timers

            closed = self.closed
            close_one = self.close_one
            wait = self.wait
            listeners_events = self.listeners_events
            process_listener_event = self.process_listener_event

            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    close_one(closed.pop(-1))

                sleep_time = fire_timers(self.clock())

                if sleep_time is not None:
                    # sleep time is an adjusted due time
                    sleep_time -= self.clock()
                    if sleep_time < 0:
                        sleep_time = 0
                else:
                    sleep_time = self.default_sleep()

                wait(sleep_time)

                # Process all fds events
                while listeners_events:
                    process_listener_event(listeners_events.popleft())

            else:
                del self.timers[:]
        finally:
            self.running = False
            self.stopping = False
        #

    def prepare_timers(self):
        pass

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
                return exp + self.timer_delay
            self.timer_delay = (due + self.timer_delay) / 2  # delay is negative value

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

    def process_listener_event(self, listener):
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
        #

    def add_timer(self, timer):
        timer.scheduled_time = self.clock() + timer.seconds
        heappush(self.timers, (timer.scheduled_time, timer))
        return timer
