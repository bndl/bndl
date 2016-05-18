.PHONY: clean, test, codecheck, sdist, bdist, upload

clean:
	find bndl -name '*.pyc' -exec rm -f {} +
	find bndl -name '*.pyo' -exec rm -f {} +
	find bndl -name '*.c' -exec rm -f {} +
	find bndl -name '*.so' -exec rm -f {} +
	find bndl -name '*~' -exec rm -f {} +
	find bndl -name '__pycache__' -exec rm -rf {} +
	rm -rf build
	rm -rf dist

test:
	venv/bin/py.test --cov-report html --cov=bndl bndl

codestyle:
	pylint bndl > build/pylint.html
	flake8 bndl > build/flake8.txt

sdist:
	venv/bin/python setup.py sdist

bdist:
	venv/bin/python setup.py bdist_egg

upload:
	python2 setup.py sdist upload -r tgho

