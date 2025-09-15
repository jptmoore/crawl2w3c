PYTHONPATH := src

.PHONY: run-filter run-generate

run-results:
	PYTHONPATH=$(PYTHONPATH) python3 scripts/results.py

run-main:
	PYTHONPATH=$(PYTHONPATH) python3 scripts/main.py
