import IPython


header = ''' _____ _____ _____ ____  __    _____ 
| __  |  |  |   | |    \|  |  |   __|
| __ -|  |  | | | |  |  |  |__|   __|
|_____|_____|_|___|____/|_____|_____|

'''


def main():
    # import here because the import involves setting up the context
    from bndl.compute.script import ctx
    try:
        IPython.embed(header=header, user_ns=dict(ctx=ctx))
    finally:
        ctx.stop()


if __name__ == '__main__':
    main()
