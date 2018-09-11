import heapq
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

        processors = (self.process_timer_event, self.process_listener_event)

        closed = self.closed
        closed_pop = self.closed.pop
        close_one = self.close_one

        events = self.timers
        next_events = self.next_timers
        next_events_pop = next_events.pop
        delay = 0
        try:
            while not self.stopping:
                chk = True
                when = self.clock()
                while not self.stopping and (chk or next_events):

                    # Ditch all closed fds first.
                    while closed:
                        close_one(closed_pop(-1))

                    while next_events:
                        event = next_events_pop(-1)
                        if hasattr(event, 'scheduled_time'):
                            # timer
                            if not event.called:
                                heappush(events, (event.scheduled_time, event))
                        else:
                            # listener event
                            ts, listener = event
                            heappush(events, (ts, listener))
                    if not events:
                        break

                    # current evaluated event
                    exp, event = events[0]
                    if isinstance(event, self.lclass):
                        typ = 1
                        # print (typ, exp, event.fileno, event.evtype)
                    else:
                        if event.called:
                            # remove called/cancelled timer
                            heappop(events)
                            continue
                        typ = 0

                    due = exp - when
                    if due > 0:
                        chk = False
                        ev_sleep(0)
                        continue
                    delay = (due + delay) / 2  # delay is negative value

                    # remove evaluated event
                    heappop(events)

                    # process event
                    processors[typ](event)

                    # check for events
                    # if readers or writers:
                    #    wait(0)

                # wait for events , until due timer or notified for fd events
                if events:
                    sleep_time = events[0][0] - self.clock() + delay
                    if sleep_time <= 0:
                        ev_sleep(0)
                        continue
                else:
                    ev_sleep(0)
                    sleep_time = self.DEFAULT_SLEEP

                # print ('events', sleep_time, len(events)) # goes mils of fd events, ?
                wait(sleep_time)
                wait_clear()

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
