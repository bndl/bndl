class Lifecycle(object):
    def __init__(self):
        self._listeners = set()
        self._is_started = False
        self._is_stopped = False
        self._cancelled = False

    def add_listener(self, listener):
        self._listeners.add(listener)

    def remove_listener(self, listener):
        self._listeners.remove(listener)

    def cancel(self):
        self._cancelled = True
        self._signal_stop()

    def _signal_start(self):
        self._is_started = True
        for l in self._listeners:
            l(self)

    def _signal_stop(self):
        self._is_stopped = True
        for l in self._listeners:
            l(self)

    @property
    def running(self):
        return self._is_started and not self._is_stopped

    @property
    def stopped(self):
        return self._is_stopped


    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop('_listeners', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self._listeners = set()
