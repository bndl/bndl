# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import site

from flask.blueprints import Blueprint
from werkzeug import redirect

from bndl.util import dash


for prefix in site.PREFIXES:
    static_folder = os.path.join(prefix, 'docs','bndl', 'html')
    if os.path.exists(static_folder):
        break
else:
    static_folder = '../../../docs/build/html'

blueprint = Blueprint('docs', __name__, template_folder='templates',
                      static_folder=static_folder, static_url_path='')

class Dash(dash.Dash):
    blueprint = blueprint

@blueprint.route('/')
def index():
    return redirect('docs/index.html')
