from datetime import datetime, timedelta
import sys
import traceback

from bndl.util import dash
from flask.blueprints import Blueprint
from flask.templating import render_template
from werkzeug.exceptions import NotFound
import flask


blueprint = Blueprint('execute', __name__,
                      template_folder='templates')


class Status(dash.StatusPanel):
    def render(self):
        return dash.status.OK, render_template('execute/status.html')


class Dash(dash.Dash):
    blueprint = blueprint
    status_panel_cls = Status


@blueprint.app_template_filter('task_stats')
def task_stats(tasks):
    total = len(tasks)

    started = [task.started_on for task in tasks if task.started_on]
    started_on = min(started) if started else None
    started = len(started)

    stopped = [task.stopped_on for task in tasks if task.stopped_on]
    stopped_on = max(stopped) if stopped else None
    stopped = len(stopped)
    all_stopped = stopped == total
    non_running = stopped == started

    running = sum(1 for task in tasks if task.started_on and not task.stopped_on)
    cancelled = sum(1 for task in tasks if task.cancelled)
    failed = sum(1 for task in tasks if task.failed)

    completed = stopped - failed - cancelled
    remaining = 0 if all_stopped else total - stopped - failed
    idle = total - started

    if started:
        if non_running:
            duration = stopped_on - started_on
        else:
            duration = datetime.now() - started_on
    else:
        duration = None

    if completed and remaining and running:
        # get durations for tasks stopped and those currently running
        durations_stopped = [task.duration for task in tasks if task.stopped_on]
        durations_running = [task.duration for task in tasks if task.started_on and not task.stopped_on]
        # sum them together
        duration_stopped = sum(durations_stopped, timedelta())
        duration_running = sum(durations_running, timedelta())
        # for the initial tasks use the duration of the tasks still running as well
        # it will be an underestimate, but less of an underestimate than using duration_stopped
        if duration_running > duration_stopped:
            avg_duration = (duration_stopped + duration_running) / (len(durations_stopped) + len(durations_running))
        else:
            avg_duration = duration_stopped / len(durations_stopped)
        # use the average duration times remaining to compute the total time remaining
        # divided by no remaining - it indicates the concurrency
        # count the duration running only half (add a bit of pessimism)
        time_remaining = max(timedelta(), avg_duration * remaining / running - duration_running / 2)
    else:
        time_remaining = None

    if non_running:
        finished_on = stopped_on
    elif time_remaining:
        finished_on = datetime.now() + time_remaining
    else:
        finished_on = ''

    return locals()


@blueprint.app_template_filter('task_status')
def task_status(task):
    if task.failed:
        return 'failed'
    elif task.cancelled:
        return 'cancelled'
    elif task.stopped_on:
        return 'done'
    elif task.started_on:
        return 'running'
    else:
        return ''


@blueprint.app_template_filter('fmt_exc')
def fmt_exc(exc):
    if sys.version_info >= (3, 5):
        return ''.join(traceback.TracebackException.from_exception(exc).format())
    else:
        parts = []
        while exc:
            etype = type(exc)
            ename = etype.__qualname__
            emod = etype.__module__
            if emod not in ("__main__", "builtins"):
                ename = emod + '.' + ename

            chunks = (
                ['Traceback (most recent call last):\n'] +
                traceback.format_tb(exc.__traceback__) +
                [ename + ':' + str(exc)]
            )
            parts.insert(0, ''.join(chunks))
            exc = exc.__cause__
        return '\n\nThe above exception was the direct cause of the following exception:\n\n'.join(parts)


@blueprint.route('/')
def jobs():
    return render_template('execute/jobs.html')


def job_by_id(job_id):
    job_id = int(job_id)
    for job in flask.g.ctx.jobs:
        if job.id == job_id:
            return job


@blueprint.route('/job/<job_id>/')
def job(job_id):
    job = job_by_id(job_id)
    if job:
        return render_template('execute/job.html', job=job)
    else:
        return NotFound()


@blueprint.route('/job/<job_id>/group/<group_id>')
def group(job_id, group_id):
    job = job_by_id(job_id)
    if job:
        tasks = job.group(int(group_id))
        if tasks:
            return render_template('execute/group.html', job=job, tasks=tasks)
    return NotFound()
