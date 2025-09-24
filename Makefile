PYTHONPATH := src

.PHONY: run-filter run-generate

run-results:
	PYTHONPATH=/app/src python3 /app/scripts/results.py

run-main:
	PYTHONPATH=/app/src python3 /app/scripts/main.py
