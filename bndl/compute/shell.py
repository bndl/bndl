import signal

import IPython


header = ''' _____ _____ _____ ____  __    _____ 
| __  |  |  |   | |    \|  |  |   __|
| __ -|  |  | | | |  |  |  |__|   __|
|_____|_____|_|___|____/|_____|_____|

'''


def on_interrupt(ctx, sig, frame):
    assert sig == signal.SIGINT
#     for job in ctx.jobs:
#         job.cancel()


def main():
    from bndl.compute.script import ctx
    try:
        # signal.signal(signal.SIGINT, functools.partial(on_interrupt, ctx))
        IPython.embed(user_ns=dict(ctx=ctx))
    finally:
        ctx.stop()  # @UndefinedVariable


if __name__ == '__main__':
    main()
