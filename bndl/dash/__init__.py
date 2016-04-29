from collections import OrderedDict
import errno
from functools import lru_cache
import logging
import threading

from bndl.dash import status
from flask import Flask
import flask
from flask.templating import render_template
from werkzeug.utils import import_string


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

# toolbar = DebugToolbarExtension(app)


dashes = OrderedDict()
def _load_dashes():
    for key, dash in app.config['BNDL_DASHES']:
        dash = import_string(dash)
        if not dash:
            logger.warning('unable to load bndl dash %s', dash)
            continue
        dashes[key] = dash
        if dash.blueprint:
            app.register_blueprint(dash.blueprint, url_prefix='/' + key)


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


def run(node=None, ctx=None):
    _load_dashes()

    @app.before_request
    def before_request():
        flask.g.node = node
        flask.g.ctx = ctx
        flask.g.dashes = dashes

    threading.Thread(target=_run, daemon=True).start()


def _run():
    for port in range(8080, 8100):
        try:
            app.run(host='0.0.0.0', port=port, debug=True, use_reloader=False)
            print('running dash on', port)
            break
        except OSError as e:
            if e.errno != errno.EADDRINUSE:
                raise e

