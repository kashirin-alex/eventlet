import eventlet
from eventlet.hubs import active_hub
import six
__select = eventlet.patcher.original('select')
error = __select.error


__patched__ = ['select']
__deleted__ = ['devpoll', 'poll', 'epoll', 'kqueue', 'kevent']


def get_fileno(obj):
    # The purpose of this function is to exactly replicate
    # the behavior of the select module when confronted with
    # abnormal filenos; the details are extensively tested in
    # the stdlib test/test_select.py.
    try:
        f = obj.fileno
    except AttributeError:
        if not isinstance(obj, six.integer_types):
            raise TypeError("Expected int or long, got %s" % type(obj))
        return obj
    else:
        rv = f()
        if not isinstance(rv, six.integer_types):
            raise TypeError("Expected int or long, got %s" % type(rv))
        return rv


def select(read_list, write_list, error_list, timeout=None):
    # error checking like this is required by the stdlib unit tests
    if timeout is not None:
        try:
            timeout = float(timeout)
        except ValueError:
            raise TypeError("Expected number for timeout")

    hub = active_hub.inst
    current = eventlet.getcurrent()
    assert hub.greenlet is not current, 'do not call blocking functions from the mainloop'

    ds_read = {get_fileno(l): l for l in read_list}
    ds_write = {get_fileno(l): l for l in write_list}
    # ds_error = {get_fileno(l): l for l in error_list}
    # or assign
    # for l in error_list:
    #   if l.evtype == hub.READ:
    #        ds_read[get_fileno(l)] = l
    #    elif l.evtype == hub.WRITE:
    #        ds_write[get_fileno(l)] = l

    current_switch = current.switch
    timers = []

    def on_read(d):
        current_switch(([ds_read[get_fileno(d)]], [], []))

    def on_write(d):
        current_switch(([], [ds_write[get_fileno(d)]], []))

    def on_timeout2():
        current_switch(([], [], []))

    def on_timeout():
        # ensure that BaseHub.run() has a chance to call self.wait()
        # at least once before timed out.  otherwise the following code
        # can time out erroneously.
        #
        # s1, s2 = socket.socketpair()
        # print(select.select([], [s1], [], 0))
        timers.append(hub.schedule_call_global(0, on_timeout2))

    if timeout is not None:
        timers.append(hub.schedule_call_global(timeout, on_timeout))

    throw = current.throw
    listeners = [
        hub.add(ev, fileno, on, throw, lambda: None)
        for fds, ev, on in (
            (ds_read, hub.READ, on_read),
            (ds_write, hub.WRITE, on_write)
        )
        for fileno in fds
    ]

    try:
        try:
            return hub.switch()
        finally:
            for l in listeners:
                hub.remove(l)
    finally:
        for t in timers:
            t.cancel()
