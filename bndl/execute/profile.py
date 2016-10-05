import os
import sys
import weakref

from cytoolz import pluck
import yappi


COLMUMNS = (
    ('name', 88),
    ('ncall', 8),
    ('tsub', 8),
    ('ttot', 8),
    ('tavg', 8),
)


class Profiling(object):
    def __init__(self, ctx):
        self.ctx = weakref.proxy(ctx)


    def start(self):
        self._each(yappi.start)


    def stop(self):
        self._each(yappi.stop)


    def get_stats(self, per_worker=False):
        individual_stats = self._each(yappi.get_func_stats)
        if per_worker:
            return individual_stats
        else:
            stat, *rest = pluck(1, individual_stats)
            # merging adapted from _add_from_YSTAT
            for other in rest:
                for saved_stat in other:
                    if saved_stat not in stat:
                        stat._idx_max += 1
                        saved_stat.index = stat._idx_max
                        stat.append(saved_stat)
                # fix children's index values
                for saved_stat in other:
                    for saved_child_stat in saved_stat.children:
                        # we know for sure child's index is pointing to a valid stat in saved_stats
                        # so as saved_stat is already in sync. (in above loop), we can safely assume
                        # that we shall point to a valid stat in current_stats with the child's full_name
                        saved_child_stat.index = stat[saved_child_stat.full_name].index
                # merge stats
                for saved_stat in other:
                    saved_stat_in_curr = stat[saved_stat.full_name]
                    saved_stat_in_curr += saved_stat
            return stat


    def print_stats(self, max_rows=100, sort_by=None, sort_dir=None,
                    columns=COLMUMNS, per_worker=False, strip_dirs=True,
                    include=(), exclude=(), file=sys.stdout):
        '''
        :param max_rows: int
            The maximum number of rows to print. Set to None to print all.
        :param sort_by: str or None
            The field to sort by, e.g. 'subtime' or 'tottime'
        :param sort_dir: 'asc', 'desc' or None
            Sort direction (ascending or descending).
        :param columns: sequence of (name:str, width:int) tuples
            The names and widths (in characters) of the columns to print.
        :param per_worker: bool
            Whether to print per worker individualy or to print the totals.
            Defaults to printing totals.
        :param strip_dirs: bool
            Whether to strip directories (only show packages / modules and line
            numbers). Defaults to True.
        :param include: str sequence
            Limit stats to these (root) modules. E.g. 'bndl' will yield any
            modules under bndl, e.g. bndl.util.
            Requires that strip_dirs is True.
        :param exclude: str sequence
            Filter out these (root) modules. E.g. 'bndl' will yield any
            modules except thos under bndl, e.g. bndl.util.
            Requires that strip_dirs is True.
        :param file: fileobj
            Where to print to. Defaults to sys.stdout.
        '''
        columns = dict(enumerate(columns))

        def print_stats(stats):
            if sort_by:
                stats.sort(sort_by, sort_dir or 'desc')
            if strip_dirs:
#                 cwd_parent = os.path.dirname(os.getcwd())
                cwd_parent = os.getcwd()
                for stat in stats:
                    if stat.full_name.startswith(cwd_parent):
                        stat.full_name = os.path.relpath(stat.full_name)
                    else:
                        for p in sys.path:
                            if stat.full_name.startswith(p):
                                stat.full_name = stat.full_name.replace(p, '').lstrip('/')
                                break
                if include or exclude:
                    excl = [mod.replace('.', '/') for mod in
                            ((exclude,) if isinstance(exclude, str) else exclude)]
                    incl = [mod.replace('.', '/') for mod in
                            ((include,) if isinstance(include, str) else include)]
                    i = 0
                    while True:
                        try:
                            stat = list.__getitem__(stats, i)
                        except IndexError:
                            break
                        if incl:
                            remove = True
                            for mod in incl:
                                if stat.full_name.startswith(mod):
                                    remove = False
                                    break
                        else:
                            remove = False
                        for mod in excl:
                            if stat.full_name.startswith(mod):
                                remove = True
                                break
                        if remove:
                            list.__delitem__(stats, i)
                        else:
                            i += 1
            if max_rows:
                del stats[max_rows:]
            stats.print_all(columns=columns, out=file)

        stats = self.get_stats(per_worker)

        if per_worker:
            for worker, stats in sorted(stats):
                print('Worker:', worker.name, end='', file=file)
                print_stats(stats)
                print('', file=file)
        else:
            print_stats(stats)


    def _each(self, func):
        tasks = [
            (worker, worker.run_task(lambda w: func()))
            for worker in self.ctx.workers
        ]
        return [(worker, task.result()) for worker, task in tasks]
