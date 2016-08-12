import copy

import IPython

from bndl.compute.run import create_ctx
from bndl.net.run import argparser
from bndl.util.conf import Config
from bndl.util.exceptions import catch


HEADER = r''' ___ _  _ ___  _    
| _ ) \| |   \| |
| _ \ .` | |) | |__
|___/_|\_|___/|____|
                    '''




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
        IPython.embed(header=HEADER, user_ns=dict(ctx=ctx))
    finally:
        with catch():
            ctx.stop()


if __name__ == '__main__':
    main()
