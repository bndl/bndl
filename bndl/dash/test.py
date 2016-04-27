from bndl.dash import app


def main():
    from bndl.compute.script import ctx
    try:
        app.run(host='localhost', port=8080, debug=True)
    finally:
        ctx.stop()


if __name__ == '__main__':
    main()



