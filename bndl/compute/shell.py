import IPython

from bndl.compute.script import ctx


header = ''' _____ _____ _____ ____  __    _____ 
| __  |  |  |   | |    \|  |  |   __|
| __ -|  |  | | | |  |  |  |__|   __|
|_____|_____|_|___|____/|_____|_____|

'''


def main():
    try:
        # as ctx is lazy, access an attribute to start the driver
        ctx.worker_count
        IPython.embed(header=header, user_ns=dict(ctx=ctx))
    finally:
        ctx.stop()


if __name__ == '__main__':
    main()
