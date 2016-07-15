CASSANDRA_VERSION ?= 3.7
ELASTICSEARCH_VERSION ?= 2.3.3

.PHONY: clean test codestyle sdist bdist upload install-test-dependencies-python install-cassandra start-cassandra stop-cassandra install-elastic start-elastic stop-elastic install-test-dependencies start-test-dependencies stop-test-dependencies test-ci  

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
	BNDL_SUPERVISOR_ONSIGTERM=raise_exit \
	COVERAGE_PROCESS_START=.coveragerc \
	coverage run -m pytest bndl
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



install-test-dependencies-python:
	pip install cassandra-driver elasticsearch-dsl
	pip install -e .[dev]



install-elastic:
	test -d /tmp/elasticsearch-$(ELASTICSEARCH_VERSION) || \
	curl https://download.elasticsearch.org/elasticsearch/release/org/elasticsearch/distribution/tar/elasticsearch/$(ELASTICSEARCH_VERSION)/elasticsearch-$(ELASTICSEARCH_VERSION).tar.gz \
	| tar -xz -C /tmp

start-elastic: install-elastic
	nohup /tmp/elasticsearch-$(ELASTICSEARCH_VERSION)/bin/elasticsearch > elastic.log 2>&1 & echo $$! > /tmp/elastic.pid

stop-elastic:
	kill `cat /tmp/elastic.pid` || :



install-cassandra:
	pip install ccm
	ccm list | grep bndl_test || ccm create bndl_test -v binary:$(CASSANDRA_VERSION) -n 1

start-cassandra: install-cassandra
	ccm switch bndl_test
	ccm start --wait-for-binary-proto

stop-cassandra:
	ccm stop || :
	ccm remove || :



install-test-dependencies: install-test-dependencies-python install-elastic install-cassandra
start-test-env: install-test-dependencies start-elastic start-cassandra
stop-test-env: stop-elastic stop-cassandra

