import importlib
import signal
import sys
import os


def exit_handler(sig, frame):
    assert sig != signal.SIGINT
    sys.exit(sig)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, signal.SIG_IGN)
    if os.environ.get('BNDL_SUPERVISOR_ONSIGTERM') == 'raise_exit':
        signal.signal(signal.SIGTERM, exit_handler)
    script, module, main, *args = sys.argv
    sys.argv = [script] + args
    module = importlib.import_module(module)
    main = getattr(module, main)
    main()
