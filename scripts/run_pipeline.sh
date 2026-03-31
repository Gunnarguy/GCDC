#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
export PYTHONPATH="$PROJECT_ROOT/src${PYTHONPATH:+:$PYTHONPATH}"

if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Missing virtual environment at $PYTHON_BIN"
  echo "Run scripts/bootstrap.sh first."
  exit 1
fi

exec "$PYTHON_BIN" -m grandchase_meta_analyzer.cli pipeline "$@"
