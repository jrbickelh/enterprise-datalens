# Makefile for Enterprise DataLens

.PHONY: setup build test clean

setup:
	# --no-install-project ensures uv doesn't look for a src/ directory
	uv sync --frozen --no-install-project --all-extras

build:
	# Running via 'uv run' will now respect the [tool.uv] package = false setting
	uv run python build_lakehouse.py
	uv run python semantic_indexer.py

test: setup build
	@echo "--> Running Smoke Test..."
	uv run python datalens_engine.py --sql "SELECT count(*) FROM transactions"
	@echo "✅ CI Simulation Passed!"

clean:
	rm -rf lakehouse lancedb _delta_log *.duckdb *.log
