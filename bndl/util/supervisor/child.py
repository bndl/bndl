import sys
import importlib

if __name__ == '__main__':
    import signal
    signal.signal(signal.SIGINT, signal.SIG_IGN)

    script, module, main, *args = sys.argv
    sys.argv = [script] + args
    module = importlib.import_module(module)
    main = getattr(module, main)
    main()
