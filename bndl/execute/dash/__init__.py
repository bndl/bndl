from flask.blueprints import Blueprint
from flask.templating import render_template
from bndl import dash
import flask
import traceback
from werkzeug.exceptions import NotFound


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
