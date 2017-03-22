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

from bndl.compute.context import ComputeContext
from bndl.compute.driver import Driver
from bndl.compute.worker import start_worker
from bndl.util.exceptions import catch
from bndl.util.funcs import noop


def create_ctx():
    driver = start_worker(Worker=Driver)

    stop = noop

    try:
        ctx = ComputeContext(driver)

        from bndl.util import dash
        dash.run(driver, ctx)

        def stop():
            dash.stop()
            driver.stop_async().result(5)

        def maybe_stop(obj):
            if obj is ctx and ctx.stopped:
                stop()

        ctx.add_listener(maybe_stop)

        return ctx
    except Exception:
        stop()
        with catch():
            ctx.stop()
        raise
