# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from collections import OrderedDict
from datetime import datetime


NOTSET = object()


class Lifecycle(object):
    def __init__(self, name=None, desc=None):
        self.started_listeners = OrderedDict()
        self.stopped_listeners = OrderedDict()
        self.started_on = None
        self.stopped_on = None
        self.cancelled = False
        self.name = name
        self.desc = desc

    def add_listener(self, started=None, stopped=NOTSET):
        if started is not None:
            self.started_listeners[started] = started
        if stopped is NOTSET:
            stopped = started
        if stopped:
            self.stopped_listeners[stopped] = stopped

    def remove_listener(self, *listeners):
        for listener in listeners:
            self.started_listeners.pop(listener, None)
            self.stopped_listeners.pop(listener, None)

    def cancel(self):
        if self.started_on and not self.stopped_on:
            self.cancelled = True
            self.signal_stop()

    def signal_start(self):
        self.started_on = datetime.now()
        self.stopped_on = None
        for listener in self.started_listeners:
            listener(self)

    def signal_stop(self):
        if not self.stopped_on:
            self.stopped_on = datetime.now()
        for listener in list(self.stopped_listeners):
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
