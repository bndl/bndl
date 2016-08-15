from flask.blueprints import Blueprint
from flask.templating import render_template

from bndl.util import dash


blueprint = Blueprint('compute', __name__,
                      template_folder='templates')


class Status(dash.StatusPanel):
    def render(self):
        return dash.status.OK, render_template('compute/status.html')


class Dash(dash.Dash):
    blueprint = blueprint
    status_panel_cls = Status


@blueprint.route('/')
def index():
    return render_template('compute/dashboard.html')
