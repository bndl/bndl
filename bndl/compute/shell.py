import copy

from ..compute.run import create_ctx
from ..net.run import argparser
from ..util.conf import Config
from ..util.exceptions import catch


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
        if args.worker_count is not None:
            config['bndl.compute.worker_count'] = args.worker_count

        ctx = create_ctx(config)
        ns = dict(ctx=ctx)

        try:
            import IPython
            IPython.embed(header=HEADER, user_ns=ns)
        except ImportError:
            import code
            code.interact(HEADER, local=ns)
    finally:
        with catch():
            ctx.stop()


if __name__ == '__main__':
    main()
