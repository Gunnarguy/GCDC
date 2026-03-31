#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"

test -d "$PROJECT_ROOT/.venv" && echo "ok .venv" || echo "missing .venv"
test -f "$PROJECT_ROOT/config/config.json" && echo "ok config" || echo "missing config"
test -f "$PROJECT_ROOT/config/hero_aliases.json" && echo "ok aliases" || echo "missing aliases"
test -f "$PROJECT_ROOT/pyproject.toml" && echo "ok pyproject" || echo "missing pyproject"
test -f "$PROJECT_ROOT/scripts/run_pipeline.sh" && echo "ok pipeline script" || echo "missing pipeline script"

if [[ -x "$PYTHON_BIN" ]]; then
  "$PYTHON_BIN" -c "import requests, bs4, pandas, sqlite3; print('ok imports')"
else
  echo "skip import check; run scripts/bootstrap.sh first"
fi
