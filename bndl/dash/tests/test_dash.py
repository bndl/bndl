import re
import unittest

from bndl.compute.dataset.tests import DatasetTest
from bndl.dash import app, _load_dashes, dashes
import flask


class DashTestCase(DatasetTest):
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
        seen = set()
        queue = ['/']

        while queue:
            url = queue.pop()
            if url in seen:
                continue
            seen.add(url)
            main = self.app.get(url, follow_redirects=True)
            self.assertEqual(main.status_code, 200)
            data = main.get_data().decode('utf-8')
            links = re.findall(r'''<a.*href=["]([^"]+)["]''', data)
            for link in links:
                if link not in seen:
                    queue.append(link)


if __name__ == '__main__':
    unittest.main()
