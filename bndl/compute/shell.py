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

import argparse

from bndl.compute.run import create_ctx
from bndl.compute.worker import argparser as worker_argparser, update_config
from bndl.util.collection import sortgroupby
from bndl.util.exceptions import catch
import bndl


HEADER = r'''         ___ _  _ ___  _
Welcome | _ ) \| |   \| |
to the  | _ \ .` | |) | |__
        |___/_|\_|___/|____| shell.

Running BNDL version %s.
ComputeContext available as ctx.''' % bndl.__version__


argparser = argparse.ArgumentParser(parents=[worker_argparser])
argparser.prog = 'bndl.compute.shell'


def main():
    update_config(argparser.parse_args())

    try:
        ctx = create_ctx()
        ns = dict(ctx=ctx)
        header = HEADER

        if bndl.conf['bndl.net.seeds'] or bndl.conf['bndl.compute.executor_count']:
            with catch():
                print('Connecting with executors ...', end='\r')
                executor_count = ctx.await_executors()
                print('                             ', end='\r')
            with catch():
                node_count = sum(1 for _ in sortgroupby(ctx.executors, lambda e: e.machine))
                header += '\nConnected with %r executors on %r nodes.' % (executor_count, node_count)

        try:
            import IPython
            IPython.embed(header=header, user_ns=ns)
        except ImportError:
            import code
            code.interact(header, local=ns)
    finally:
        with catch():
            ctx.stop()


if __name__ == '__main__':
    main()
