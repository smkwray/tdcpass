VENV ?= $(HOME)/venvs/tdcpass
PYTHON ?= $(VENV)/bin/python
PIP ?= $(VENV)/bin/pip

.PHONY: install doctor demo pipeline test

install:
	$(PIP) install -e '.[dev]'

doctor:
	$(PYTHON) -B -m tdcpass doctor

demo:
	$(PYTHON) -B -m tdcpass demo

pipeline:
	$(PYTHON) -B -m tdcpass pipeline run

test:
	$(PYTHON) -B -m pytest -q
