from bndl.util.conf import Int


concurrency = Int(1)


class TaskCancelled(Exception):
    pass
