import IPython

from bndl.compute import ctx


HEADER = r''' ___ _  _ ___  _    
| _ ) \| |   \| |
| _ \ .` | |) | |__
|___/_|\_|___/|____|
                    '''


def main():
    try:
        # as ctx is lazy, access it to start the driver
        # and wait for workers to become online
        ctx.await_workers()
        IPython.embed(header=HEADER, user_ns=dict(ctx=ctx))
    finally:
        ctx.stop()


if __name__ == '__main__':
    main()
