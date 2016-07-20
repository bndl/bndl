from collections import OrderedDict
from datetime import datetime, timedelta
import errno
from functools import lru_cache
import logging
import threading

from flask import Flask
import flask
from flask.templating import render_template
from werkzeug.utils import import_string

from bndl.dash import status
import math
import werkzeug
import os
from werkzeug.serving import make_server
from threading import Event


logger = logging.getLogger(__name__)


class Dash(object):
    status_panel_cls = None
    blueprint = None


class StatusPanel(object):
    def __init__(self, application):
        self.app = application

    def render(self):
        return status.DISABLED, ''


app = Flask(__name__)
app.config.from_object('bndl.dash.settings')
_srv = None

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
    for key, dash in dashes.items():
        if dash.status_panel_cls:
            panels[key] = dash.status_panel_cls(app)
    return panels


@app.route('/')
def dash_main():
    return render_template('dash/dashboard.html',
                           status_panels=_status_panels())


@app.template_filter('filtercount')
def filtercount(seq, attr):
    return sum(1 for e in seq if getattr(e, attr, None))


@app.template_global('now')
def now():
    return datetime.now()


@app.template_filter('fmt_timedelta')
def fmt_timedelta(tdelta):
    if not tdelta:
        return ''
    elif isinstance(tdelta, (int, float)):
        if math.isnan(tdelta):
            return 'NaN'
        else:
            tdelta = timedelta(seconds=tdelta)
    parts = str(tdelta).split('.')
    if tdelta < timedelta(seconds=0.01):
        return parts[1].strip('0') + ' µs'
    elif tdelta < timedelta(seconds=1):
        return parts[1][:3].strip('0') + ' ms'
    elif tdelta < timedelta(seconds=10):
        return parts[0][-1] + '.' + parts[1][:2] + ' s'
    else:
        return parts[0]


def run(node=None, ctx=None):
    _load_dashes()

    @app.before_request
    def before_request():
        flask.g.node = node
        flask.g.ctx = ctx
        flask.g.dashes = dashes

    logging.getLogger('werkzeug').setLevel(logging.WARN)
    started = Event()
    threading.Thread(target=_run, args=(started,),
                     daemon=True, name='bndl-dash-thread').start()
    started.wait()


def stop():
    global _srv
    if _srv:
        try:
            _srv.shutdown()
        except Exception:
            pass

def _run(started):
    for port in range(8080, 8100):
        try:
            global _srv
            _srv = make_server('0.0.0.0', port, app)
            started.set()
            _srv.serve_forever()
            break
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise exc
