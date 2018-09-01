import os

from eventlet import patcher
from eventlet.support import greenlets as greenlet
import six


__all__ = ["active_hub", "use_hub", "get_hub", "get_default_hub", "trampoline"]

threading = patcher.original('threading')
_threadlocal = threading.local()
if hasattr(_threadlocal, 'hub'):
    del _threadlocal.hub

hub_priorities = [os.environ.get('EVENTLET_HUB', None), 'epolls', 'poll', 'selects']  # , 'pyevent'


def get_default_hub(mod=None):
    """Select the default hub implementation based on what multiplexing
    libraries are installed.  The order that the hubs are tried is:

    * epoll
    * kqueue
    * poll
    * select

    It won't automatically select the pyevent hub, because it's not
    python-thread-safe.

    .. include:: ../doc/common.txt
    .. note :: |internal|


        Mod can be an actual module, a string, or None.  If *mod* is a module,
        it uses it directly.   If *mod* is a string and contains either '.' or ':'
        use_hub tries to import the hub using the 'package.subpackage.module:Class'
        convention, otherwise use_hub looks for a matching setuptools entry point
        in the 'eventlet.hubs' group to load or finally tries to import
        `eventlet.hubs.mod` and use that as the hub module.

    """
    selected_mod = None
    if mod is not None and not isinstance(mod, six.string_types):
        selected_mod = mod
    else:
        for m in [mod] + hub_priorities:
            if m is None:
                continue
            try:
                if '.' in m or ':' in m:
                    modulename, _, classname = mod.strip().partition(':')
                    selected_mod = __import__(modulename, globals(), locals(), [classname])
                    if classname:
                        selected_mod = getattr(mod, classname)
                    break
                selected_mod = __import__('eventlet.hubs.' + mod, globals(), locals(), ['Hub'])
            except Exception as e:
                print (e)
            
    assert selected_mod is not None, "Need to specify a hub"
    return selected_mod


class HubHolder:
    inst = None  # active hub instance

    @classmethod
    def __init__(cls):
        if cls.inst is None:
            cls.use_hub()
        #

    @classmethod
    def get_hub(cls):
        """Get the current event hub singleton object.
                    .. note :: |internal|
                    """
        if cls.inst is None:
            cls.__init__()
        return cls.inst
        #

    @classmethod
    def use_hub(cls, mod=None):
        """Use the module *mod*, containing a class called Hub, as the
        event hub. Usually not required; the default hub is usually fine.
        If *mod* is None, use_hub uses the default hub.  Only call use_hub during application
        initialization,  because it resets the hub's state and any existing
        timers or listeners will never be resumed.
        """
        mod = get_default_hub(mod)
        _threadlocal.Hub = getattr(mod, 'Hub', mod)
        cls.inst = _threadlocal.Hub()
        #

active_hub = HubHolder()
get_hub = HubHolder.get_hub  # intermediate ref
use_hub = HubHolder.use_hub  # intermediate ref


# Lame middle file import because complex dependencies in import graph
from eventlet import timeout


def trampoline(fd, read=None, write=None, timeout=None,
               timeout_exc=timeout.Timeout,
               mark_as_closed=None):
    """Suspend the current coroutine until the given socket object or file
    descriptor is ready to *read*, ready to *write*, or the specified
    *timeout* elapses, depending on arguments specified.

    To wait for *fd* to be ready to read, pass *read* ``=True``; ready to
    write, pass *write* ``=True``. To specify a timeout, pass the *timeout*
    argument in seconds.

    If the specified *timeout* elapses before the socket is ready to read or
    write, *timeout_exc* will be raised instead of ``trampoline()``
    returning normally.

    .. note :: |internal|
    """
    t = None
    hub = active_hub.inst
    current = greenlet.getcurrent()
    assert hub.greenlet is not current, 'do not call blocking functions from the mainloop'
    assert not (
        read and write), 'not allowed to trampoline for reading and writing'
    try:
        fileno = fd.fileno()
    except AttributeError:
        fileno = fd
    if timeout is not None:
        def _timeout(exc):
            # This is only useful to insert debugging
            current.throw(exc)
        t = hub.schedule_call_global(timeout, _timeout, timeout_exc)
    try:
        if read:
            listener = hub.add(hub.READ, fileno, current.switch, current.throw, mark_as_closed)
        elif write:
            listener = hub.add(hub.WRITE, fileno, current.switch, current.throw, mark_as_closed)
        else:
            listener = None
        try:
            return hub.switch()
        finally:
            hub.remove(listener)
    finally:
        if t is not None:
            t.cancel()


class IOClosed(IOError):
    pass
