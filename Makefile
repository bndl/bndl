.PHONY: clean test codestyle

clean:
	find bndl -name '*.pyc' -exec rm -f {} +
	find bndl -name '*.pyo' -exec rm -f {} +
	find bndl -name '*.c' -exec rm -f {} +
	find bndl -name '*.so' -exec rm -f {} +
	find bndl -name '*~' -exec rm -f {} +
	find bndl -name '__pycache__' -exec rm -rf {} +
	rm -rf build
	rm -rf dist
	rm -rf .coverage .coverage.* htmlcov

test:
	rm -fr .coverage .coverage.* htmlcov
	COVERAGE_PROCESS_START=.coveragerc \
	coverage run -m pytest bndl
	coverage combine
	coverage html

codestyle:
	pylint bndl > build/pylint.html
	flake8 bndl > build/flake8.txt
