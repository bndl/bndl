import importlib
import signal
import sys
import traceback


def exit_handler(sig, frame):
    assert sig != signal.SIGINT
    sys.exit(sig)

def excepthook(etype, value, tb):
    if etype != SystemExit:
        traceback.print_exception(etype, value, tb)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, exit_handler)
    sys.excepthook = excepthook

    script, module, main, *args = sys.argv
    sys.argv = [script] + args
    module = importlib.import_module(module)
    main = getattr(module, main)
    main()
