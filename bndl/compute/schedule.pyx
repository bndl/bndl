from collections import deque, Iterable, OrderedDict

from bndl.compute.dataset import ComputePartitionTask, Partition


cdef tuple generate_tasks(tasks, dset, int group, int groups):
    dset_tasks = [ComputePartitionTask(part, group)
                  for part in dset.parts()]

    stack = deque()
    src = dset.src
    if src:
        if isinstance(src, Iterable):
            stack.extend(src)
        else:
            stack.append(src)

    while stack:
        d = stack.popleft()
        if d.sync_required:
            cached = d.cached and d._cache_locs
            groups, dependencies = generate_tasks(tasks, d, group + 1, max(groups, group+1))
            for task in dset_tasks:
                task.dependencies.extend(dependencies)
            for dependency in dependencies:
                if cached:
                    dependency.mark_done()
                for task in dset_tasks:
                    dependency.dependents.append(task)
        else:
            d_src = d.src
            if d_src:
                if isinstance(d_src, Iterable):
                    stack.extend(d_src)
                else:
                    stack.append(d_src)

    for task in dset_tasks:
        tasks[task.id] = task

    return groups, dset_tasks


def schedule(dset):
    tasks = OrderedDict()
    groups, _ = generate_tasks(tasks, dset, 1, 1)
    taskslist = []
    for task in tasks.values():
        task.group = groups - task.group + 1
        taskslist.append(task)
    return taskslist
