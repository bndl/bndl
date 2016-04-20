.PHONY: test

clean:
	find bndl -name '*.pyc' -exec rm -f {} +
	find bndl -name '*.pyo' -exec rm -f {} +
	find bndl -name '*.c' -exec rm -f {} +
	find bndl -name '*.so' -exec rm -f {} +
	find bndl -name '*~' -exec rm -f {} +
	find bndl -name '__pycache__' -exec rm -rf {} +
	rm -rf bndl.egg-info
	rm -rf build
	rm -rf dist

ccm-create:
	ccm create -n 5 -s bndl -v 2.2.5 --vnodes

test:
	venv/bin/python setup.py test

test-py:
	venv/bin/py.test bndl

test-py-cov:
	venv/bin/py.test --cov-report html --cov=bndl bndl

sdist:
	venv/bin/python setup.py sdist

bdist:
	venv/bin/python setup.py bdist_egg

