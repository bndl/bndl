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
