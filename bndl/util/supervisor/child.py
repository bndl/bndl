import importlib
import signal
import sys


def exit_handler(sig, frame):
    assert sig != signal.SIGINT
    sys.exit(sig)


def main():
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    signal.signal(signal.SIGTERM, exit_handler)
    script, module, main, *args = sys.argv
    sys.argv = [script] + args
    module = importlib.import_module(module)
    main = getattr(module, main)
    main()


if __name__ == '__main__':
    main()
