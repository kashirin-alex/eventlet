from eventlet.hubs.v1_base import HubBase


class BaseHub(HubBase):
    """ Base hub class for easing the implementation of subclasses that are
    specific to a particular underlying event architecture. """

    def __init__(self, clock=None):
        super(BaseHub, self).__init__(clock)
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

            timers = self.timers
            fire_timers = self.fire_timers
            prepare_timers = self.prepare_timers

            closed = self.closed
            close_one = self.close_one
            wait = self.wait
            listeners_events = self.listeners_events
            listeners_events_popleft = self.listeners_events.popleft
            process_listener_event = self.process_listener_event

            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    close_one(closed.pop(-1))

                prepare_timers()
                fire_timers(self.clock())
                prepare_timers()

                if timers:
                    sleep_time = timers[0][0] - self.clock() + self.timer_delay
                    if sleep_time < 0:
                        sleep_time = 0
                else:
                    sleep_time = self.default_sleep()
                wait(sleep_time)

                # Process all fds events
                while listeners_events:
                    process_listener_event(listeners_events_popleft())

            else:
                del self.timers[:]
                del self.next_timers[:]
                del self.listeners_events[:]
                del self.closed[:]
        finally:
            self.running = False
            self.stopping = False
        #
