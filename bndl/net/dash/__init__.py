from flask import g
from flask.blueprints import Blueprint
from flask.templating import render_template

from bndl import dash as bndl_dash


blueprint = Blueprint('net', __name__,
                      template_folder='templates')


class Status(bndl_dash.StatusPanel):
    @property
    def status(self):
        if not g.node.running:
            return bndl_dash.status.ERROR
        if len(g.node.peers.filter()) < len(g.node.peers):
            return bndl_dash.status.WARNING
        else:
            return bndl_dash.status.OK


    def render(self):
        return render_template('net/status.html')


class Dash(bndl_dash.Dash):
    blueprint = blueprint
    status_panel_cls = Status


@blueprint.route('/')
def index():
    return render_template('net/dashboard.html')
