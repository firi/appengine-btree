GAE=/Applications/GoogleAppEngineLauncher.app/Contents/Resources/GoogleAppEngine-default.bundle/Contents/Resources/google_appengine/
GAEPATH=$(GAE):$(GAE)/lib/yaml/lib:$(GAE)/lib/webob:$(GAE)/lib/fancy_urllib:$(GAE)/lib/simplejson
PYTHON= python -Wignore
COVERAGE=/opt/local/Library/Frameworks/Python.framework/Versions/2.7/bin/coverage
NONTESTS=`find btree -name [a-z]\*.py ! -name \*_test.py`

default: test

test:
	@PYTHONPATH=$(GAEPATH):. $(PYTHON) -m btree.btree_test

coverage:
	@PYTHONPATH=$(GAEPATH):. ${COVERAGE} run -m btree.btree_test $(FLAGS)
	$(COVERAGE) html $(NONTESTS)
	$(COVERAGE) report -m $(NONTESTS)

