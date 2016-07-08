from unittest.case import TestCase

from bndl.util import supervisor


def test_entry_point():
    pass

class TestSupervisor(TestCase):
    def test_supervisor_main(self):
        supervisor.main(supervisor.argparser.parse_args(['bndl.util.tests.test_supervisor:test_entry_point']))