PYTHON ?= python

install:
	$(PYTHON) -m pip install -e .[dev]

lint:
	$(PYTHON) -m compileall src scripts tests

test:
	pytest -q

ingest-bluesky:
	$(PYTHON) scripts/ingest_bluesky.py

ingest-x-notes:
	$(PYTHON) scripts/ingest_x_notes.py

ingest-reddit:
	$(PYTHON) scripts/ingest_reddit.py

launch:
	$(PYTHON) scripts/launch_pipeline.py

process:
	$(PYTHON) scripts/process_batch.py

viewer-install:
	cd viewer && uv sync

viewer-test:
	cd viewer && uv run pytest tests -q

viewer-build:
	docker compose -f viewer/docker-compose.yml build

viewer-up:
	docker compose -f viewer/docker-compose.yml up

viewer-down:
	docker compose -f viewer/docker-compose.yml down
