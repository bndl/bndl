from datetime import datetime
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
def task_stats(tasklist):
    tasks = tasklist.tasks
    total = len(tasks)

    started = sum(1 for t in tasks if t.started_on)
    stopped = sum(1 for t in tasks if t.stopped_on)
    cancelled = sum(1 for t in tasks if t.cancelled)
    failed = sum(1 for t in tasks if t.failed)
    running = sum(1 for t in tasks if t.started_on and not t.stopped_on)

    completed = stopped - cancelled
    remaining = 0 if tasklist.stopped_on else total - stopped - cancelled
    idle = total - started

    duration = ((tasklist.stopped_on or datetime.now()) - tasklist.started_on) if tasklist.started_on else None
    time_remaining = (duration / stopped * total - duration if duration and stopped and remaining else None)
    finished_on = tasklist.stopped_on or (tasklist.started_on + duration + time_remaining if time_remaining else '')
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

def stage_by_id(job, stage_id):
    stage_id = int(stage_id)
    for stage in job.stages:
        if stage.id == stage_id:
            return stage


@blueprint.route('/job/<job_id>/')
def job(job_id):
    job = job_by_id(job_id)
    if job:
        return render_template('execute/job.html', job=job)
    else:
        return NotFound()


@blueprint.route('/job/<job_id>/stage/<stage_id>')
def stage(job_id, stage_id):
    job = job_by_id(job_id)
    if job:
        stage = stage_by_id(job, stage_id)
        if stage:
            return render_template('execute/stage.html', job=job, stage=stage)
    return NotFound()
