import threading
import concurrent.futures

class OnDemandThreadedExecutor(concurrent.futures.Executor):
    '''
    An minimal - almost primitive - Executor, that spawns a thread per task.
    
    Used only because concurrent.futures.ThreadPoolExecutor isn't able to
    scale down the number of active threads, deciding on a maximum number of
    concurrent tasks may be difficult and keeping max(concurrent tasks) threads
    lingering around seems wasteful.
    '''
    
    def submit(self, fn, *args, **kwargs):
        future = concurrent.futures.Future()
        def work():
            try:
                result = fn(*args, **kwargs)
                future.set_result(result)
            except Exception as exception:
                future.set_exception(exception)
        threading.Thread(target=work).start()
        return future
