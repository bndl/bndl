from concurrent.futures import TimeoutError
import argparse
import asyncio
import atexit
import logging
import os
import signal
import subprocess
import sys
import time

from bndl.compute.blocks import BlockManager
from bndl.compute.broadcast import BroadcastManager
from bndl.compute.memory import  LocalMemoryManager, MemoryCoordinator
from bndl.compute.shuffle import ShuffleManager
from bndl.compute.tasks import Tasks
from bndl.net.aio import get_loop, get_loop_thread, run_coroutine_threadsafe, stop_loop
from bndl.net.rmi import RMINode
from bndl.net.run import argparser as net_argparser
from bndl.util.threads import dump_threads
import bndl


logger = logging.getLogger(__name__)


# If an executor fails within MIN_RUN_TIME seconds, the executor is considered unstable and isn't
# rebooted.
MIN_RUN_TIME = 10


# When a executor exits with one of the following return codes, it is not revived. 0 indicates the
# process exited 'normally'
# A process may be killed with SIGTERM or SIGKILL, and exit with -SIGTERM / -SIGKILL, this is
# considered an action by a parent process and/or administrator.
# The convention is maintained that if a executor wants to perform cleanup (e.g. run atexit
# functionality) it exits with the SIGTERM or SIGKILL codes. Note that also the time between exit
# and the last start is also taken into account, see MIN_RUN_TIME.
DNR_CODES = 0, -signal.SIGTERM, -signal.SIGKILL, signal.SIGTERM, signal.SIGKILL


class ExecutorMonitor(object):
    def __init__(self, executor_id, worker, executable):
        self.executor_id = executor_id
        self.loop = worker.loop
        self.executable = executable
        self.running = False
        self.proc = None
        self.started_on = None


    @asyncio.coroutine
    def start(self):
        self.running = True
        yield from self._create_process()
        self.loop.create_task(self.watch())


    @asyncio.coroutine
    def stop(self):
        if self.running:
            self.running = False
            try:
                self.proc.terminate()
                yield from self.proc.wait()
            except ProcessLookupError:
                pass


    @asyncio.coroutine
    def _create_process(self):
        env = dict(os.environ)

        # Make sure hashes aren't salted so that an object gets the same hash across processes
        env['PYTHONHASHSEED'] = '0'

        # Make sure numpy, scipy etc don't ask OMP to create as much threads as there are cores as
        # BNDL is already parallelizing work.
        env['OMP_NUM_THREADS'] = '2'

        self.proc = yield from asyncio.create_subprocess_exec(
            *self.executable, loop=self.loop, stdin=None, stdout=None, stderr=None, env=env
        )

        self.started_on = time.time()


    @asyncio.coroutine
    def watch(self):
        while self.running:
            yield from self.proc.wait()

            restart = self.proc.returncode not in DNR_CODES and \
                      (time.time() - self.started_on) > MIN_RUN_TIME

            logger.log(logging.ERROR if restart else logging.INFO,
                       'Executor %s (pid %s) exited with code %s, %srestarting',
                       self.executor_id, self.proc.pid, self.proc.returncode,
                       '' if restart else 'not ')

            if restart:
                yield from self._create_process()
            else:
                break



class Worker(RMINode):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._monitors = []

        self.services['blocks'] = BlockManager(self)
        self.services['broadcast'] = BroadcastManager(self)
        self.services['shuffle'] = ShuffleManager(self)
        self.services['tasks'] = Tasks(self)

        self.memory_manager = LocalMemoryManager()
        self.memory_coordinator = MemoryCoordinator(self.peers)
        self.memory_coordinator.add_memory_manager(
            os.getpid(),
            lambda nbytes: self.memory_manager.release(self, nbytes)
        )


    @property
    def executors(self):
        '''All peers of the local node with ``node_type`` `worker`.'''
        return self.peers.filter(node_type='executor')


    @asyncio.coroutine
    def start(self):
        yield from super().start()
        self.memory_manager.start()
        self.memory_coordinator.start()


    def start_executors(self, n_executors=None):
        return run_coroutine_threadsafe(self._start_executors(n_executors), self.loop)


    @asyncio.coroutine
    def await_executors(self, max_wait=1, min_count=None):
        if min_count == 0:
            return
        pids = lambda: [m.proc.pid for m in self._monitors if m.proc]
        min_count = min_count or len(pids())
        remaining = max_wait
        wait = remaining / 1000
        deadline = time.monotonic() + max_wait
        while True:
            if len(self.peers.filter(pid=pids())) >= min_count:
                break
            yield from asyncio.sleep(min(wait, remaining), loop=self.loop)
            remaining = deadline - time.monotonic()
            wait *= 2
            if remaining < 0:
                break
        return len(self.peers.filter(pid=pids()))


    @asyncio.coroutine
    def _start_executors(self, n_executors=None):
        if n_executors is None:
            n_executors = bndl.conf['bndl.compute.executor_count']

        numactl = bndl.conf['bndl.run.numactl']
        pincore = bndl.conf['bndl.run.pincore']
        jemalloc = bndl.conf['bndl.run.jemalloc']

        if numactl and not pincore:
            n_zones = len(subprocess.check_output(['numastat | head -n 1'], shell=True).split())
            numactl = n_zones > 1

        if pincore:
            n_cores = os.cpu_count()
            pincore = n_cores > 1

        for i in range(1, n_executors + 1):
            if numactl:
                node = str(sum(self.id) % n_zones)
                executable = ['numactl', '-N', node, '--preferred', node]
            elif pincore:
                core = str(sum(self.id) % n_cores)
                executable = ['taskset', '-c', core]
            else:
                executable = []

            if jemalloc:
                executable = ['jemalloc.sh'] + executable

            executable += [sys.executable, '-m', 'bndl.compute.executor',
                           self.cluster, self.machine,
                           self.addresses[0], str(len(self._monitors) + 1)]

            emon = ExecutorMonitor(i, self, executable)
            self._monitors.append(emon)

            yield from emon.start()
            yield from self.await_executors(MIN_RUN_TIME // 100, max(0, i // 2))

        yield from self.await_executors(MIN_RUN_TIME)


    @asyncio.coroutine
    def stop(self):
        self.memory_manager.stop()
        self.memory_coordinator.stop()

        yield from asyncio.gather(
            *(emon.stop() for emon in self._monitors), loop=self.loop
        )

        yield from super().stop()



def _check_command_exists(name):
    try:
        subprocess.check_output(['which', name], stderr=subprocess.DEVNULL)
    except Exception:
        return False
    else:
        return True


NUMACTL_AVAILBLE = _check_command_exists('numactl') and _check_command_exists('numastat')
JEMALLOC_AVAILABLE = _check_command_exists('jemalloc.sh')
TASKSET_AVAILABLE = _check_command_exists('taskset')


argparser = argparse.ArgumentParser(add_help=False, parents=[net_argparser])
argparser.epilog = 'Use -- after the supervisor arguments to separate them from' \
                   'arguments to the main program.'


if NUMACTL_AVAILBLE:
    argparser.add_argument('--numactl', dest='numactl', action='store_true',
                                help='Bind processes round-robin to NUMA zones with numactl.')
    argparser.add_argument('--no-numactl', dest='numactl', action='store_false',
                                help='Don\'t attempt to bind processes to a NUMA zone with numactl')

if TASKSET_AVAILABLE:
    argparser.add_argument('--pincore', dest='pincore', action='store_true',
                                help='Pin processes round-robin to a specific core with taskset')
    argparser.add_argument('--no-pincore', dest='pincore', action='store_false',
                                help='Don\'t pin processes to specific cores with taskset')

if JEMALLOC_AVAILABLE:
    argparser.add_argument('--jemalloc', dest='jemalloc', action='store_true',
                                help='Use jemalloc.sh (if available on the PATH).')
    argparser.add_argument('--no-jemalloc', dest='jemalloc', action='store_false',
                                help='Don\'t attempt to use jemalloc.sh (use the system default '
                                     'malloc, usually malloc from glibc).')


argparser.set_defaults(numactl=bndl.conf['bndl.run.numactl'] if NUMACTL_AVAILBLE else False)
argparser.set_defaults(pincore=bndl.conf['bndl.run.pincore'] if TASKSET_AVAILABLE else False)
argparser.set_defaults(jemalloc=bndl.conf['bndl.run.jemalloc'] if JEMALLOC_AVAILABLE else False)


argparser.add_argument('--executor-count', dest='executor_count', nargs='?', type=int,
                       help='The number of BNDL executors to start (defaults to 0 if seeds is set).')
argparser.add_argument('--conf', nargs='*', default=(),
                       help='BNDL configuration in "key=value" format')


def update_config(args=None):
    args = args or argparser.parse_args()

    conf = bndl.conf
    conf.update(*args.conf)

    conf['bndl.run.numactl'] = args.numactl
    conf['bndl.run.pincore'] = args.pincore
    conf['bndl.run.jemalloc'] = args.jemalloc

    if args.listen_addresses:
        conf['bndl.net.listen_addresses'] = args.listen_addresses

    if args.seeds:
        conf['bndl.net.seeds'] = args.seeds
        if not conf.is_set('bndl.compute.executor_count'):
            conf.setdefault('bndl.compute.executor_count', 0)

    if args.executor_count:
        conf['bndl.compute.executor_count'] = args.executor_count



HEADER = r'''         ___ _  _ ___  _
Running | _ ) \| |   \| |
        | _ \ .` | |) | |__
        |___/_|\_|___/|____| worker.

Running BNDL version %s.''' % bndl.__version__


def start_worker(Worker=Worker, n_executors=None, verbose=0):
    if n_executors is None:
        n_executors = bndl.conf['bndl.compute.executor_count']

    if verbose > 0:
        if n_executors:
            print('Starting BNDL worker with', n_executors, 'executors ...', end='\r')
        else:
            print('Starting BNDL worker...', end='\r')

    loop = get_loop(start=True)
    worker = Worker(loop=loop)
    worker.start_async().result()

    if n_executors:
        worker.start_executors(n_executors).result()

    if verbose > 0:
        print(' ' * 80, end='\r')
        print(HEADER)
        if n_executors:
            print('Started', n_executors, 'executors locally')
        print('Listening on', *worker.addresses)
        seeds = [a for a in (bndl.conf['bndl.net.seeds'] or ()) if a not in worker.addresses]
        if seeds:
            print('Joining cluster via', *seeds)
        print('-' * 80)

    @atexit.register
    def stop(*args):
        if worker.running and loop.is_running():
            try:
                run_coroutine_threadsafe(worker.stop(), loop).result(1)
            except TimeoutError:
                pass
        stop_loop()

    def exit_handler(sig, frame):
        stop()
        sys.exit(sig)

    signal.signal(signal.SIGTERM, exit_handler)
    signal.signal(signal.SIGUSR1, dump_threads)

    return worker


def main(args=None):
    if not args:
        args = argparse.ArgumentParser(parents=[argparser]).parse_args()

    update_config(args)

    start_worker(verbose=1, n_executors=args.executor_count or os.cpu_count())

    try:
        get_loop_thread().join()
    except KeyboardInterrupt:
        pass



if __name__ == '__main__':
    main()
