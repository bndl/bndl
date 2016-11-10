from datetime import datetime


class Lifecycle(object):
    def __init__(self, name=None, desc=None):
        self.started_listeners = set()
        self.stopped_listeners = set()
        self.started_on = None
        self.stopped_on = None
        self.cancelled = False
        self.name = name
        self.desc = desc

    def add_listener(self, started, stopped=None):
        self.started_listeners.add(started)
        self.stopped_listeners.add(stopped or started)

    def remove_listener(self, *listeners):
        for listener in listeners:
            self.started_listeners.discard(listener)
            self.stopped_listeners.discard(listener)

    def cancel(self):
        if self.started_on and not self.stopped_on:
            self.cancelled = True
            self.signal_stop()

    def signal_start(self):
        if not self.started_on:
            self.started_on = datetime.now()
        for listener in self.started_listeners:
            listener(self)

    def signal_stop(self):
        if not self.stopped_on:
            self.stopped_on = datetime.now()
        for listener in self.stopped_listeners:
            listener(self)

    @property
    def running(self):
        return self.started_on and not self.stopped_on

    @property
    def stopped(self):
        return bool(self.stopped_on)

    @property
    def duration(self):
        if self.started_on:
            if self.stopped_on:
                return self.stopped_on - self.started_on
            else:
                return datetime.now() - self.started_on


    def __getstate__(self):
        state = dict(self.__dict__)
        state.pop('started_listeners', None)
        state.pop('stopped_listeners', None)
        state.pop('desc', None)
        return state

    def __setstate__(self, state):
        self.__dict__.update(state)
        self.started_listeners = set()
        self.stopped_listeners = set()
