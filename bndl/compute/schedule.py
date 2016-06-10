from collections import Iterable, Sized
from functools import partial
from itertools import chain
import logging
import traceback

from bndl.execute.job import Job, Stage, Task


logger = logging.getLogger(__name__)


def schedule_job(dset, workers=None):
    '''
    Schedule a job for a data set
    :param dset:
        The data set to schedule
    '''

    ctx = dset.ctx
    assert ctx.running, 'context of dataset is not running'

    ctx.await_workers()
    workers = ctx.workers[:]

    job = Job(ctx, *_job_calling_info())

    stage = Stage(None, job)
    schedule_stage(stage, workers, dset)
    job.stages.insert(0, stage)

    def _cleaner(dset, job):
        if job.stopped:
            dset.cleanup(job)

    while dset:
        if dset.cleanup:
            job.add_listener(partial(_cleaner, dset))
        if isinstance(dset.src, Iterable):
            for src in dset.src:
                branch = schedule_job(src)

                for task in branch.stages[-1].tasks:
                    task.args = (task.args[0], True)

                for listener in branch.listeners:
                    job.add_listener(listener)

                if dset.sync_required:
                    branch_stages = branch.stages
                elif len(branch.stages) > 1:
                    branch_stages = branch.stages[:-1]
                else:
                    continue

                for stage in reversed(branch_stages):
                    stage.job = job
                    stage.is_last = False
                    job.stages.insert(0, stage)

            break

        elif dset.sync_required:
            stage = stage.prev_stage = Stage(None, job)
            schedule_stage(stage, workers, dset)
            job.stages.insert(0, stage)

        dset = dset.src

    # Since stages are added in reverse, setting the ids in execution order
    # later in execution order gives a clearer picture to users
    for idx, stage in enumerate(job.stages):
        stage.id = idx

    for task in job.stages[-1].tasks:
        task.args = (task.args[0], True)

    return job


def _job_calling_info():
    name = None
    desc = None
    for file, lineno, func, text in reversed(traceback.extract_stack()):
        if 'bndl/' in file and func[0] != '_':
            name = func
        desc = file, lineno, func, text
        if 'bndl/' not in file:
            break
    return name, desc



def _get_cache_loc(part):
    loc = part.dset._cache_locs.get(part.idx)
    if loc:
        return loc
    elif part.src:
        if isinstance(part.src, Iterable):
            return set(chain.from_iterable(_get_cache_loc(src) for src in part.src))
        else:
            return _get_cache_loc(part.src)


def schedule_stage(stage, workers, dset):
    '''
    Schedule a stage for a data set.

    It is assumed that all source data sets (and their parts) are materialized when
    this data set is materialized. (i.e. parts call materialize on their sources,
    if any).

    Also it is assumed that stages are scheduled backwards. Specifically if
    stage.is_last when this function is called it will remain that way ...

    :param stage: Stage
        stage to add tasks to
    :param workers: list or set
        Workers to schedule the data set on
    :param dset:
        The data set to schedule
    '''
    stage.name = dset.__class__.__name__

    for part in dset.parts():
        allowed_workers = list(part.allowed_workers(workers) or [])
        preferred_workers = list(part.preferred_workers(allowed_workers or workers) or [])

        stage.tasks.append(MaterializePartitionTask(
            part, stage,
            preferred_workers, allowed_workers
        ))

    # sort the tasks by their id
    stage.tasks.sort(key=lambda t: t.id)


class MaterializePartitionTask(Task):
    def __init__(self, part, stage,
                 preferred_workers, allowed_workers,
                 name=None, desc=None):
        self.part = part
        super().__init__(
            (part.dset.id, part.idx),
            stage,
            materialize_partition, (part, False), None,
            preferred_workers, allowed_workers,
            name, desc)

    def result(self):
        result = super().result()
        self._save_cacheloc(self.part)
        self.part = None
        return result

    def _save_cacheloc(self, part):
        # memorize the cache location for the partition
        if part.dset.cached:
            part.dset._cache_locs[part.idx] = self.executed_on[-1]
        # traverse backup up the DAG
        if part.src:
            if isinstance(part.src, Iterable):
                for src in part.src:
                    self._save_cacheloc(src)
            else:
                self._save_cacheloc(part.src)


def is_stable_iterable(obj):
    '''
    This rule is supposed to catch generators, islices, map objects and the
    lot. They aren't serializable unless materialized in e.g. a list are there
    cases where a) an unserializable type is missed? or b) materializing data
    into a list is a bad (wrong result, waste of resources, etc.)? numpy arrays
    are not wrongly cast to a list through this. That's something ...
    :param obj: The object to test
    '''
    return (
        (isinstance(obj, Iterable) and isinstance(obj, Sized))
        or isinstance(obj, type({}.items()))
    )

def materialize_partition(worker, part, return_data):
    try:
        ctx = part.dset.ctx

        # generate data
        data = part.materialize(ctx)

        # return data if requested
        if return_data and data is not None:
            # 'materialize' iterators and such for pickling
            if is_stable_iterable(data):
                return list(data)
            else:
                return data
    except:
        logger.info('error while materializing part %s on worker %s',
                    part, worker, exc_info=True)
        raise
