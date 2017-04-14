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

from bndl.compute.context import ComputeContext
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
        ctx = ComputeContext.get_or_create()
        ns = dict(ctx=ctx)
        header = HEADER

        seeds = bndl.conf['bndl.net.seeds']
        executor_count = bndl.conf['bndl.compute.executor_count']

        if seeds or executor_count:
            executor_count = 0
            node_count = 0
            with catch():
                if seeds:
                    print('Connecting with executors through %s seed%s ...' %
                          (len(seeds), 's' if len(seeds) > 1 else ''), end='\r')
                else:
                    print('Connecting with local executors ...', end='\r')
                executor_count = ctx.await_executors()
                print('                             ' * 120, end='\r')
            with catch():
                node_count = sum(1 for _ in sortgroupby(ctx.executors, lambda e: e.machine))
                header += '\nConnected with %r executors' % executor_count
                if node_count:
                    header += ' on %r nodes' % node_count
                header += '.'

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
