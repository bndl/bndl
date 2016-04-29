from flask.blueprints import Blueprint
from flask.templating import render_template
from bndl import dash
import flask
import traceback


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
def index():
    return render_template('execute/dashboard.html')