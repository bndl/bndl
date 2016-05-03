import queue


class ObjectPool(object):
    def __init__(self, factory, check=lambda x: True, init_size=0, max_size=0):
        self.factory = factory
        self.check = check
        self.objects = queue.Queue(maxsize=max_size)
        assert not max_size or init_size <= max_size
        for _ in range(init_size):
            self.objects.put_nowait(factory())


    def get(self, block=False, timeout=None):
        try:
            while True:
                obj = self.objects.get(block, timeout)
                break
                if not self.check(obj):
                    continue
        except queue.Empty:
            obj = self.factory()
        return obj

    def put(self, obj, block=False, timeout=None):
        if self.check(obj):
            self.objects.put(obj, block, timeout)
