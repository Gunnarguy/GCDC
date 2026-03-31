PYTHON := .venv/bin/python
PIP := .venv/bin/pip

.PHONY: bootstrap pipeline scrape normalize pages llm-tag notebook explorer lint test

bootstrap:
	python3 -m venv .venv
	$(PYTHON) -m pip install --upgrade pip
	$(PIP) install -r requirements.txt

pipeline:
	PYTHONPATH=src $(PYTHON) -m grandchase_meta_analyzer.cli pipeline

scrape:
	PYTHONPATH=src $(PYTHON) -m grandchase_meta_analyzer.cli scrape

normalize:
	PYTHONPATH=src $(PYTHON) -m grandchase_meta_analyzer.cli normalize

pages:
	PYTHONPATH=src $(PYTHON) -m grandchase_meta_analyzer.cli pages

llm-tag:
	PYTHONPATH=src $(PYTHON) -m grandchase_meta_analyzer.cli tag-skills

notebook:
	$(PYTHON) -m jupyter notebook notebooks/analysis.ipynb

explorer:
	PYTHONPATH=src $(PYTHON) -m grandchase_meta_analyzer.cli explorer

lint:
	$(PYTHON) -m ruff check src tests

test:
	$(PYTHON) -m pytest
