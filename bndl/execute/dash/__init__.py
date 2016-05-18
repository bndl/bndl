from datetime import datetime

import flask
from flask.blueprints import Blueprint
from flask.templating import render_template
from werkzeug.exceptions import NotFound

from bndl import dash


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
    running = sum(1 for t in tasks if t.started_on and not t.stopped_on)
    remaining = len(tasks) - stopped
    idle = len(tasks) - started
    duration = ((tasklist.stopped_on or datetime.now()) - tasklist.started_on) if tasklist.started_on else None
    time_remaining = (duration / stopped * len(tasks) - duration if duration and stopped and remaining else None)
    finished = tasklist.stopped_on or (tasklist.started_on + time_remaining if time_remaining else '')
    return locals()


@blueprint.route('/')
def jobs():
    return render_template('execute/jobs.html')


def job_by_id(job_id):
    job_id = int(job_id)
    for job in flask.g.ctx.jobs:
        if job.id == job_id:
            return job


@blueprint.route('/job/<job_id>')
def job(job_id):
    job = job_by_id(job_id)
    if job:
        return render_template('execute/job.html', job=job)
    else:
        return NotFound()
