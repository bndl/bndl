from subprocess import Popen
import sys
import time

args = sys.argv[1:]
if args:
    print(args)
else:
    start = time.time()
    proc = Popen(args=[sys.executable, __file__, 'hi!'])
    proc.wait()
    print('subprocess done in', time.time() - start)
