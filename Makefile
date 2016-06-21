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
	rm -f .coverage

test:
	rm -f .coverage
	COVERAGE_PROCESS_START=.coveragerc coverage run -m pytest bndl
	coverage combine
	coverage html

codestyle:
	pylint bndl > build/pylint.html
	flake8 bndl > build/flake8.txt

sdist:
	python setup.py sdist

bdist:
	python setup.py bdist_egg

upload:
	python2 setup.py sdist upload -r tgho

