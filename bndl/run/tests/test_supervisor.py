from unittest.case import TestCase

from bndl.run import supervisor
import os
import sys
import time
import tempfile


MIN_RUN_TIME = .1


def test_entry_point():
    # write out the pid file if requested
    pidfile = os.environ.get('TestSupervisor.pidfile')
    if pidfile is not None:
        with open(pidfile, 'a') as f:
            print(os.getpid(), file=f)

    # exit with exit code if env var set
    exitcode = os.environ.get('TestSupervisor.exitcode')
    if exitcode is not None:
        # and sleep a bit to give supervisor the 'impression' the child was 'stable'
        time.sleep(MIN_RUN_TIME * 2)
        sys.exit(exitcode)


class TestSupervisor(TestCase):
    def setUp(self):
        # create a tmp file to collect pids of children in
        self.pidfile_fd, self.pidfile_name = tempfile.mkstemp()
        os.environ['TestSupervisor.pidfile'] = self.pidfile_name
        # really make sure the temp file is empty before the test is started
        with open(self.pidfile_name, 'r') as f:
            self.assertEqual(len(f.read()), 0)


    def tearDown(self):
        # close the 'pidfile'
        os.close(self.pidfile_fd)


    def get_pids(self):
        with open(self.pidfile_name, 'r') as f:
            return [int(line) for line in f.read().split('\n') if line]


    def test_supervisor_watcher(self):
        # communicate to test_entry_point to exit
        os.environ['TestSupervisor.exitcode'] = '1'

        # run the supervisor
        sup = supervisor.Supervisor('bndl.util.tests.test_supervisor', 'test_entry_point', [], 1,
                         min_run_time=.1, check_interval=.01)
        sup.start()
        # and wait at least three times
        for _ in range(3):
            sup.wait()
            time.sleep(MIN_RUN_TIME * 2)
        # finally stop
        sup.stop()

        # compare the 'pidfile' with the expect number of runs
        # (should be at least 3, maybe more ... timing not that precise)
        # and that the last pid of the first and only child
        # matches the last pid in the pidfile
        pids = self.get_pids()
        self.assertTrue(len(pids) >= 3, 'Only started %s times' % len(pids))
        self.assertEqual(pids[-1], sup.children[0].proc.pid)
