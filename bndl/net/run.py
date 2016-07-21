import argparse
import asyncio

from bndl.net.connection import urlcheck


argparser = argparse.ArgumentParser()
argparser.add_argument('--listen_addresses', nargs='*', type=urlcheck)
argparser.add_argument('--seeds', nargs='*', type=urlcheck)


def exception_handler(loop, context):
    exc = context.get('exception')
    if type(exc) is not SystemExit:
        loop.default_exception_handler(context)


def run_nodes(*nodes, started_signal=None, stop_signal=None):
    assert len(nodes) > 0

    loop = nodes[0].loop
    assert all(node.loop == loop for node in nodes)

    try:
        current_loop = asyncio.get_event_loop()
    except:
        # set the event loop for this thread if not set yet
        asyncio.set_event_loop(loop)
        current_loop = loop

    loop.set_exception_handler(exception_handler)

    # check that we're running on the correct loop
    # (always the case if we've set the event loop)
    assert loop == current_loop

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
            raise e

    try:
        if stop_signal:
            loop.run_until_complete(loop.run_in_executor(None, stop_signal.result))
        else:
            loop.run_forever()
    finally:
        stops = [node.stop() for node in nodes]
        loop.run_until_complete(asyncio.wait(stops, loop=loop))
