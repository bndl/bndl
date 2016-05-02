import IPython

from bndl.compute.script import ctx


header = ''' ___ _  _ ___  _    
| _ ) \| |   \| |   
| _ \ .` | |) | |__ 
|___/_|\_|___/|____|
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
