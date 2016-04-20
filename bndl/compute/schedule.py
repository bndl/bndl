import collections.abc as collections_abc
import collections
import logging

from bndl.execute.job import Job, Stage, Task


logger = logging.getLogger(__name__)


def schedule_job(dset):
    '''
    Schedule a job for a data set
    :param dset:
        The data set to schedule
    '''

    ctx = dset.ctx
    assert ctx.running, 'context of dataset is not running'
    ctx._await_workers()
    workers = ctx.workers[:]
    job = Job(ctx)

    stage = Stage(None, job)
    schedule_stage(stage, workers, dset)
    job.stages.insert(0, stage)

    while dset:
        if isinstance(dset.src, collections_abc.Iterable):
            for src in dset.src:
                branch = schedule_job(src)

                if dset.sync_required:
                    branch_stages = branch.stages
                elif len(branch.stages) > 1:
                    # raise Exception('untested code!')
                    branch_stages = branch.stages[:-1]
                else:
                    break

                # branch_stages[-1].next_stage = job.stages[-1]
                for stage in reversed(branch_stages):
                    stage.job = job
                    stage.is_last = False
                    job.stages.insert(0, stage)

            break

        elif dset.sync_required:
            stage = stage.prev_stage = Stage(None, job)  # , next_stage=stage)
            schedule_stage(stage, workers, dset)
            job.stages.insert(0, stage)

        dset = dset.src

    # Since stages are added in reverse, setting the ids in execution order
    # later in execution order gives a clearer picture to users
    for idx, stage in enumerate(job.stages):
        stage.id = idx

    for task in job.stages[-1].tasks:
        task.args[1] = True

    return job



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
    :param workers: iterable
        Workers to schedule the data set on
    :param dset:
        The data set to schedule
    '''

    assignments = collections.defaultdict(lambda: [])
    assignment_count = lambda worker: (len(assignments[worker]), worker.name)

    for part in dset.parts():
        allowed_workers = list(part.preferred_workers(workers) or [])
        preferred_workers = list(part.preferred_workers(allowed_workers or workers) or [])

        # sort the allowed and preferred workers, least assigned first
        allowed_workers.sort(key=assignment_count)
        preferred_workers.sort(key=assignment_count)

        # select one worker to execute the task on
        selected_worker = (preferred_workers or allowed_workers or [None])[0]
        if not selected_worker:
            selected_worker = sorted(workers, key=assignment_count)[0]

        # use it as preferred worker if no provided by partition
        if not preferred_workers:
            preferred_workers = [selected_worker]

        # and update assignments
        assignments[selected_worker].append((part, (allowed_workers, preferred_workers)))

    # loop over assignments per worker
    for _, parts in assignments.items():
        # and task per assigned part
        for part, (allowed_workers, preferred_workers) in parts:
            stage.tasks.append(Task(
                (part.dset.id, part.idx),
                stage,
                materialize_partition, [part, False], None,
                preferred_workers, allowed_workers
            ))

    # sort the tasks by their id
    stage.tasks.sort(key=lambda t: t.id)



def materialize_partition(worker, part, return_data):
    try:
        ctx = part.dset.ctx
        _set_worker(part.dset, worker)

        data = part.materialize(ctx)
        if return_data and data is not None:
            # TODO This rule is supposed to catch generators, islices, map
            # objects and the lot. They aren't serializable unless materialized
            # in e.g. a list are there cases where a) an unserializable type is
            # missed? or b) materializing data into a list is a bad (wrong
            # result, waste of resources, etc.)? numpy arrays are not wrongly
            # cast to a list through this. That's something ...
            if isinstance(data, collections_abc.Iterable) and not isinstance(data, collections_abc.Sized):
                return list(data)
            else:
                return data
    except Exception:
        logger.info('error while materializing part %s on worker %s',
                    part, worker, exc_info=True)
        raise

def _set_worker(dset, worker):
    dset.ctx.node = worker
    if isinstance(dset.src, collections_abc.Iterable):
        for dset in dset.src:
            _set_worker(dset, worker)
    elif dset.src:
        _set_worker(dset.src, worker)

