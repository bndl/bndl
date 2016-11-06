import copy

from bndl.compute.run import create_ctx
from bndl.net.run import argparser
from bndl.util.conf import Config
from bndl.util.exceptions import catch
from toolz.itertoolz import groupby
from bndl.util.funcs import identity


HEADER = r'''         ___ _  _ ___  _
Welcome | _ ) \| |   \| |
to the  | _ \ .` | |) | |__
        |___/_|\_|___/|____| shell.

ComputeContext available as ctx.'''


argparser = copy.copy(argparser)
argparser.prog = 'bndl.compute.shell'
argparser.add_argument('--worker-count', nargs='?', type=int, default=None, dest='worker_count')
argparser.add_argument('--conf', nargs='*', default=())


def main():
    try:
        args = argparser.parse_args()
        config = Config({
            k.strip() : v.strip()
            for k, v in (c.split('=', 1) for c in args.conf)
        })

        if args.listen_addresses:
            config['bndl.net.listen_addresses'] = args.listen_addresses
        if args.seeds:
            config['bndl.net.seeds'] = args.seeds
            config['bndl.compute.worker_count'] = 0
        if args.worker_count is not None:
            config['bndl.compute.worker_count'] = args.worker_count

        ctx = create_ctx(config)
        ns = dict(ctx=ctx)

        if config['bndl.net.seeds'] or config['bndl.compute.worker_count']:
            print('Connecting with workers ...', end='\r')
            worker_count = ctx.await_workers(args.worker_count)
            node_count = len(groupby(identity, [tuple(sorted(worker.ip_addresses())) for worker in ctx.workers]))
            header = HEADER + '\nConnected with %r workers on %r nodes' % (worker_count, node_count)
        else:
            header = HEADER

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
