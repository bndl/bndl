import queue
import threading
from datetime import timedelta
import time


class ObjectPool(object):

    def __init__(self, factory, check=lambda x: True,
                 destroy=lambda x: None, min_size=0, max_size=0,
                 max_idle=None):
        '''
        
        :param factory: func()
            Functino to create a new obj
        :param check: func(obj)
            Optional function to check the 'fitness' of obj
        :param destroy: func(obj)
            Optional function to clean up for obj
        :param min_size: int
            The minimum and initial size of the pool. 
        :param max_size: int
            The maximum size of the pool. 
        :param max_idle: int, float or timedelta
            The maximum time an object may reside in the pool. After this time,
            the obj will be removed and destroy(obj) will be invoked.
        '''
        assert not max_size or min_size <= max_size
        self.min_size = min_size
        self.max_size = max_size
        if isinstance(max_idle, timedelta):
            self.max_idle = max_idle.total_seconds()
        else:
            self.max_idle = max_idle
        self.factory = factory
        self.check = check
        self.destroy = destroy
        self.monitor = None
        self.closed = False
        self._init_queue()


    def _init_queue(self, objects=()):
        self.objects = queue.PriorityQueue(maxsize=self.max_size)
        try:
            for obj in objects:
                self.objects.put_nowait(obj)
        except queue.Full:
            pass
        self._ensure_size()
        self._clean()
            
            
    def _ensure_size(self):
        try:
            while self.objects.qsize() < self.min_size:
                self.objects.put_nowait((time.time(), self.factory()))
        except queue.Full:
            pass


    def get(self, block=False, timeout=None):
        '''
        Take an object from the pool.
        :param block: bool
            Whether to wait for an object to be available in the pool. If
            False and the pool is empty, a new object will be created.
        :param timeout: int or None
            The time in seconds to wait if block is True.
        '''
        assert not self.closed
        try:
            while True:
                inserted, obj = self.objects.get(block, timeout)
                if self._check(inserted, obj):
                    self._ensure_size()
                    return obj
                else:
                    self.destroy(obj)
        except queue.Empty:
            obj = self.factory()
        return obj


    def put(self, obj, block=False, timeout=None):
        '''
        Return an object to the pool.
        :param obj:
            The object to return.
        :param block: bool
            Whether to wait for room in the pool. If False and the pool is full
            the object is destroyed.
        :param timeout:
            The time in seconds to wait if block is True.
        '''
        assert not self.closed
        inserted = time.time()
        if self._check(inserted, obj):
            try:
                self.objects.put((inserted, obj), block, timeout)
            except queue.Full:
                self.destroy(obj)
        else:
            self.destroy(obj)


    def close(self):
        assert not self.closed
        self.closed = True
        if self.monitor:
            self.monitor.cancel()
        while True:
            try:
                self.destroy(self.objects.get_nowait())
            except queue.Empty:
                break


    def _check(self, inserted, obj):
        if self.max_idle and time.time() - inserted > self.max_idle:
            return False
        try:
            return bool(self.check(obj))
        except Exception:
            return False


    def _clean(self):
        if self.max_idle:
            if not self.objects.empty() and not self._check(*self.objects.queue[0]):
                while True:
                    try:
                        inserted, obj = self.objects.get_nowait()
                        if self._check(inserted, obj):
                            self.objects.put_nowait((inserted, obj))
                            break
                    except queue.Empty:
                        break
            self._ensure_size()
        if not self.closed:
            self.monitor = threading.Timer(self.max_idle, self._clean)
            self.monitor.daemon = True
            self.monitor.start()


    def __getstate__(self):
        state = dict(self.__dict__)
        state['objects'] = self.objects.queue
        state['monitor'] = None
        return state


    def __setstate__(self, state):
        self.__dict__.update(state)
        self._init_queue(self.objects)
        return self
