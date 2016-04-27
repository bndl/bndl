from functools import lru_cache
import logging

from bndl.dash import status
from flask import Flask
from flask.templating import render_template
from flask_debugtoolbar import DebugToolbarExtension
from werkzeug.utils import import_string
from collections import OrderedDict
import flask


logger = logging.getLogger(__name__)





class Dash(object):
    status_panel_cls = None
    blueprint = None


class StatusPanel(object):
    status = status.OK

    def __init__(self, app):
        self.app = app

    def render(self):
        return ''


app = Flask(__name__)
app.config.from_object('bndl.dash.settings')

toolbar = DebugToolbarExtension(app)


dashes = OrderedDict()
for key, dash in app.config['BNDL_DASHES']:
    dash = import_string(dash)
    if not dash:
        logger.warning('unable to load bndl dash %s', dash)
        continue
    dashes[key] = dash
    if dash.blueprint:
        app.register_blueprint(dash.blueprint, url_prefix='/' + key)


ctx = app.app_context()
ctx.push()
ctx.g.dashes = dashes


@lru_cache()
def _status_panels():
    panels = OrderedDict()
    for key, d in dashes.items():
        if d.status_panel_cls:
            panels[key] = d.status_panel_cls(app)
    return panels


@app.route('/')
def dash_main():
    print('dash_main', dir(flask.g))
    return render_template('dash/dashboard.html',
                           status_panels=_status_panels())
