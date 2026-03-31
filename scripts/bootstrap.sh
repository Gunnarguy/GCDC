#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PYTHON_FOR_VENV="${GC_PYTHON_BIN:-}"
PYTHON_BIN="$PROJECT_ROOT/.venv/bin/python"
PIP_BIN="$PROJECT_ROOT/.venv/bin/pip"

if [[ -z "$PYTHON_FOR_VENV" ]]; then
	for candidate in /opt/homebrew/bin/python3.11 python3.11 python3; do
		if command -v "$candidate" >/dev/null 2>&1; then
			PYTHON_FOR_VENV="$(command -v "$candidate")"
			break
		fi
	done
fi

if [[ -z "$PYTHON_FOR_VENV" ]]; then
	echo "Unable to find a Python interpreter for virtual environment creation."
	exit 1
fi

"$PYTHON_FOR_VENV" -m venv "$PROJECT_ROOT/.venv"
"$PYTHON_BIN" -m pip install --upgrade pip
"$PIP_BIN" install -r "$PROJECT_ROOT/requirements.txt"

echo "Bootstrap complete. Base interpreter: $PYTHON_FOR_VENV"
echo "Virtual environment: $PYTHON_BIN"
