from flask.blueprints import Blueprint
from flask.templating import render_template
from bndl.dash import Dash, StatusPanel, status, app
import flask
import traceback


blueprint = Blueprint('compute', __name__,
                      template_folder='templates')


class ComputeStatus(StatusPanel):
    @property
    def status(self):
        return status.OK

    def render(self):
        return render_template('compute/status.html')


class ComputeDash(Dash):
    blueprint = blueprint
    status_panel_cls = ComputeStatus


@blueprint.route('/')
def index():
    return render_template('compute/dashboard.html')