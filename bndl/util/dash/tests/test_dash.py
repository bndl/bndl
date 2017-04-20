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

import re
import unittest

import flask

from bndl.compute.tests import ComputeTest
from bndl.net.rmi import InvocationException
from bndl.util.dash import app, _load_dashes, dashes


class DashTestCase(ComputeTest):
    def setUp(self):
        _load_dashes()
        @app.before_request
        def before_request():
            flask.g.node = self.ctx.node
            flask.g.ctx = self.ctx
            flask.g.dashes = dashes
        app.config['TESTING'] = True
        self.app = app.test_client()

    def test_crawl(self):
        # generate some 'pages'

        self.ctx.range(10).count()
        self.ctx.range(10).sort().count()
        with self.assertRaises(InvocationException):
            self.ctx.range(10).map(lambda i: exec('raise ValueError()')).execute()

        seen = set()
        queue = ['/']

        while queue:
            url = queue.pop()
            if url in seen:
                continue
            seen.add(url)
            with self.subTest(url):
                main = self.app.get(url, follow_redirects=True)
                self.assertEqual(main.status_code, 200)
                data = main.get_data().decode('utf-8')
                links = re.findall(r'''<a.+href=["']([^"']+)["']''', data) + re.findall(r'''<.+data-href=["']([^"']+)["']''', data)
                for link in links:
                    if 'docs' in link:
                        continue
                    if not link[0] == '/':
                        link = url + '/' + link
                    if link not in seen:
                        queue.append(link)

if __name__ == '__main__':
    unittest.main()
