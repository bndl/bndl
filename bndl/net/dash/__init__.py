from flask import g
from flask.blueprints import Blueprint
from flask.templating import render_template

from bndl.util import dash


blueprint = Blueprint('net', __name__,
                      template_folder='templates')


class Status(dash.StatusPanel):
    @property
    def status(self):
        if not g.node.running:
            return dash.status.ERROR
        if len(g.node.peers.filter()) < len(g.node.peers):
            return dash.status.WARNING
        else:
            return dash.status.OK


    def render(self):
        return self.status, render_template('net/status.html')


class Dash(dash.Dash):
    blueprint = blueprint
    status_panel_cls = Status


@blueprint.route('/')
def index():
    return render_template('net/dashboard.html')
