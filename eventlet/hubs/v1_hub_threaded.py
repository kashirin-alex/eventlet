import eventlet
from eventlet.hubs.v1_base import HubBase

orig_threading = eventlet.patcher.original('threading')
ev_sleep = eventlet.patcher.original('time').sleep


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

        listeners_events = self.listeners_events
        listeners_events_popleft = self.listeners_events.popleft
        process_listener_event = self.process_listener_event

        closed = self.closed
        closed_pop = self.closed.pop
        close_one = self.close_one

        timers = self.timers
        prepare_timers = self.prepare_timers
        fire_timers = self.fire_timers

        try:
            while not self.stopping:

                # Ditch all closed fds first.
                while closed:
                    close_one(closed_pop(-1))

                prepare_timers()
                fire_timers(self.clock())
                prepare_timers()

                if not listeners_events:
                    if timers:
                        sleep_time = timers[0][0] - self.clock() + self.timer_delay
                        if sleep_time < 0:
                            sleep_time = 0
                        else:
                            ev_sleep(0)
                    else:
                        sleep_time = self.default_sleep()
                        ev_sleep(0)

                    print ('events', len(listeners_events), sleep_time, len(timers))
                    wait(sleep_time)
                    wait_clear()
                else:
                    listeners = []
                    while listeners_events:
                        listeners.append(listeners_events_popleft())
                    for l in listeners:
                        process_listener_event(l)

            else:
                del self.timers[:]
                del self.next_timers[:]
                del self.listeners_events[:]
                del self.closed[:]
        finally:
            self.running = False
            self.stopping = False
        #
