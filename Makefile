PYTHONPATH := src

.PHONY: run-filter run-generate run-upload-existing

run-results:
	PYTHONPATH=/app/src python3 /app/scripts/results.py

run-main:
	PYTHONPATH=/app/src python3 /app/scripts/main.py

run-upload-existing:
	PYTHONPATH=/app/src python3 /app/scripts/upload_existing_results.py --default
