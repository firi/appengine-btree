GAE=../google_appengine
GAEPATH=$(GAE)
PYTHON= python -Wignore
COVERAGE=/opt/local/Library/Frameworks/Python.framework/Versions/2.7/bin/coverage
NONTESTS=`find btree -name [a-z]\*.py ! -name \*_test.py`

default: test

test:
	$(PYTHON) run_tests.py $(GAEPATH)

coverage:
	$(COVERAGE) run_tests.py $(GAEPATH)
	$(COVERAGE) html $(NONTESTS)
	$(COVERAGE) report -m $(NONTESTS)

