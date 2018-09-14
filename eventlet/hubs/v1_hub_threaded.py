import heapq
import sys
import eventlet
from eventlet.support import clear_sys_exc_info
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

        listeners = self.listeners
        timers = self.timers
        next_timers = self.next_timers
        next_timers_pop = next_timers.pop

        closed = self.closed
        close_one = self.close_one
        pop_closed = self.closed.pop

        fd_events = self.listeners_events
        pop_fd_event = fd_events.popleft

        squelch_exception = self.squelch_exception
        sys_exec = self.SYSTEM_EXCEPTIONS

        clock = self.clock
        delay = 0
        try:
            while not self.stopping:

                while next_timers:
                    t = next_timers_pop(-1)
                    if not t.called:
                        heappush(timers, (t.scheduled_time, t))

                if timers:
                    exp, t = timers[0]  # current evaluated timer
                    if t.called:
                        heappop(timers)  # remove called/cancelled timer
                        continue
                    due = exp - clock()
                    if due < 0:
                        heappop(timers)  # remove evaluated timer
                        delay += due
                        delay /= 2
                        try:
                            t()
                        except sys_exec:
                            raise
                        except:
                            pass
                    else:
                        due += delay
                else:
                    due = self.DEFAULT_SLEEP

                while closed:  # Ditch all closed fds first.
                    close_one(pop_closed(-1))

                if not fd_events and due > 0:
                    wait(due)  # wait for fd signals
                    wait_clear()

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
