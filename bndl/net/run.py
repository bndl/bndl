import argparse
import asyncio

from bndl.net.connection import urlcheck


argparser = argparse.ArgumentParser(add_help=False)
argparser.add_argument('--listen-addresses', nargs='*', type=urlcheck, dest='listen_addresses')
argparser.add_argument('--seeds', nargs='*', type=urlcheck)


def exception_handler(loop, context):
    exc = context.get('exception')
    if type(exc) is not SystemExit:
        loop.default_exception_handler(context)


def run_nodes(*nodes, started_signal=None, stop_signal=None):
    assert len(nodes) > 0

    loop = nodes[0].loop
    assert all(node.loop == loop for node in nodes)
    asyncio.set_event_loop(loop)
    loop.set_exception_handler(exception_handler)

    try:
        starts = [node.start() for node in nodes]
        loop.run_until_complete(asyncio.wait(starts, loop=loop))
        if started_signal:
            started_signal.set_result(True)
    except Exception as e:
        if started_signal:
            started_signal.set_result(e)
            return
        else:
            raise

    try:
        if stop_signal:
            loop.run_until_complete(loop.run_in_executor(None, stop_signal.result))
        else:
            loop.run_forever()
    finally:
        stops = [node.stop() for node in nodes]
        loop.run_until_complete(asyncio.wait(stops, loop=loop))
