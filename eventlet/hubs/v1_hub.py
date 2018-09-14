import sys

from eventlet.support import clear_sys_exc_info

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
            pop_closed = self.closed.pop
            close_one = self.close_one
            wait = self.wait

            fd_events = self.listeners_events
            pop_fd_event = self.listeners_events.popleft
            listeners = self.listeners

            squelch_exception = self.squelch_exception
            sys_exec = self.SYSTEM_EXCEPTIONS

            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    close_one(pop_closed(-1))

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
