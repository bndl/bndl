import logging

from bndl.compute.accumulate import AccumulatorService
from bndl.compute.worker import Worker


logger = logging.getLogger(__name__)


class Driver(Worker, AccumulatorService):
    def __init__(self, *args, **kwargs):
        Worker.__init__(self, *args, **kwargs)
        AccumulatorService.__init__(self)
