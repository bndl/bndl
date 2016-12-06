from flask.blueprints import Blueprint

from bndl.util import dash
from werkzeug import redirect


blueprint = Blueprint('docs', __name__, template_folder='templates',
                      static_folder='../../../docs/build/html', static_url_path='')

class Dash(dash.Dash):
    blueprint = blueprint

@blueprint.route('/')
def index():
    return redirect('docs/index.html')
