import argparse

from bndl.net.connection import urlparse
from bndl.util.aio import get_loop
import os


def url(string):
    try:
        urlparse(string)
        return string
    except ValueError as exc:
        raise ValueError('ill-formatted seed url:', string, 'error:', exc)


argparser = argparse.ArgumentParser()
argparser.add_argument('--listen_addresses', nargs='*', type=url)
argparser.add_argument('--seeds', nargs='*', type=url, default=['tcp://localhost:5000'])


def create_node(cls, args, loop=None):
    if not loop:
        loop = get_loop()
    return cls(loop, addresses=args.listen_addresses, seeds=args.seeds)


def run_nodecls(cls, args):
    node = create_node(cls, args)
    run_node(node)
    return node


def run_node(node):
    loop = node.loop
    loop.run_until_complete(node.start())
    try:
        while True:
            try:
                loop.run_forever()
            except KeyboardInterrupt:
                if int(os.environ.get('BNDL_IS_SUPERVISED', '0')) == 0:
                    break
    finally:
        loop.run_until_complete(node.stop())
        loop.close()
