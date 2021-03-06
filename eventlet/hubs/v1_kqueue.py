import os
import six

from eventlet import patcher
from eventlet.hubs.v1_hub import BaseHub

select = patcher.original('select')
time = patcher.original('time')


def is_available():
    return hasattr(select, 'kqueue')


class Hub(BaseHub):
    MAX_EVENTS = 100
    FILTERS = {BaseHub.READ: select.KQ_FILTER_READ,
               BaseHub.WRITE: select.KQ_FILTER_WRITE}

    def __init__(self, clock=None):
        super(Hub, self).__init__(clock)
        self._events = {}
        self._init_kqueue()

    def _init_kqueue(self):
        self.kqueue = select.kqueue()
        self._pid = os.getpid()

    def _reinit_kqueue(self):
        self.kqueue.close()
        self._init_kqueue()
        kqueue = self.kqueue
        events = [e for i in six.itervalues(self._events)
                  for e in six.itervalues(i)]
        kqueue.control(events, 0, 0)

    def _control(self, events, max_events, timeout):
        try:
            return self.kqueue.control(events, max_events, timeout)
        except (OSError, IOError):
            # have we forked?
            if os.getpid() != self._pid:
                self._reinit_kqueue()
                return self.kqueue.control(events, max_events, timeout)
            raise

    def add(self, evtype, fileno, cb, tb, mac):
        listener = self.add_listener(evtype, fileno, cb, tb, mac)
        events = self._events.setdefault(fileno, {})
        if evtype not in events:
            try:
                event = select.kevent(fileno, self.FILTERS.get(evtype), select.KQ_EV_ADD)
                self._control([event], 0, 0)
                events[evtype] = event
            except ValueError:
                self.remove_listener(listener)
                raise
        return listener

    def _delete_events(self, events):
        del_events = [
            select.kevent(e.ident, e.filter, select.KQ_EV_DELETE)
            for e in events
        ]
        self._control(del_events, 0, 0)

    def remove(self, listener):
        self.remove_listener(listener)
        evtype = listener.evtype
        fileno = listener.fileno
        if not self.listeners[evtype].get(fileno):
            event = self._events[fileno].pop(evtype, None)
            if event is None:
                return
            try:
                self._delete_events((event,))
            except OSError:
                pass

    def remove_descriptor(self, fileno):
        self.remove_descriptor_from_listeners(fileno)
        try:
            self._delete_events(self._events.pop(fileno).values())
        except KeyError:
            pass
        except OSError:
            pass

    def wait(self, seconds=None):
        if not self.listeners[self.READ] and not self.listeners[self.WRITE]:
            if seconds:
                time.sleep(seconds)
            return

        result = self._control([], self.MAX_EVENTS, seconds)
        for event in result:
            fileno = event.ident
            evfilt = event.filter
            if evfilt == self.FILTERS[self.READ]:
                self.add_listener_events((self.READ, fileno))
            if evfilt == self.FILTERS[self.WRITE]:
                self.add_listener_events((self.WRITE, fileno))
