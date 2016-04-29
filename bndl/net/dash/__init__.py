from flask.blueprints import Blueprint
from flask import g
from flask.templating import render_template
from bndl import dash as dash2
import flask
import traceback


blueprint = Blueprint('net', __name__,
                      template_folder='templates')


class Status(dash2.StatusPanel):
    @property
    def status(self):
        if not g.node.running:
            return dash2.status.ERROR
        if len(g.node.peers.filter()) < len(g.node.peers):
            return dash2.status.WARNING
        else:
            return dash2.status.OK


    def render(self):
        return render_template('net/status.html')


class Dash(dash2.Dash):
    blueprint = blueprint
    status_panel_cls = Status


@blueprint.route('/')
def index():
    return render_template('net/dashboard.html')
