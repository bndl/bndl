from flask.blueprints import Blueprint
from flask.templating import render_template
from bndl import dash
import flask
import traceback
from werkzeug.exceptions import NotFound
from bndl.dash import app
from datetime import datetime


blueprint = Blueprint('execute', __name__,
                      template_folder='templates')


class Status(dash.StatusPanel):
    @property
    def status(self):
        return dash.status.OK

    def render(self):
        return render_template('execute/status.html')


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
    duration = (tasklist.stopped_on or datetime.now()) - tasklist.started_on
    time_remaining = (duration / stopped * len(tasks) - duration if stopped and remaining else None)
    finished = tasklist.stopped_on or tasklist.started_on + time_remaining
    return locals()

@blueprint.route('/')
def jobs():
    return render_template('execute/jobs.html')

@blueprint.route('/job/<job_id>')
def job(job_id):
    job_id = int(job_id)
    job = None
    for j in flask.g.ctx.jobs:
        if j.id == job_id:
            job = j
            break

    if job:
        return render_template('execute/job.html', job=job)
    else:
        return NotFound()
