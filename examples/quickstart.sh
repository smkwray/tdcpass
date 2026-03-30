#!/usr/bin/env bash
set -euo pipefail

if [ -f .env ]; then
  set -a
  source .env
  set +a
fi

export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$HOME/venvs/tdcpass}"
export PYTHONDONTWRITEBYTECODE="${PYTHONDONTWRITEBYTECODE:-1}"
export PYTHONPYCACHEPREFIX="${PYTHONPYCACHEPREFIX:-/tmp/tdcpass-pycache}"
export PYTEST_ADDOPTS="${PYTEST_ADDOPTS:--p no:cacheprovider}"
export UV_CACHE_DIR="${UV_CACHE_DIR:-/tmp/uv-cache-tdcpass}"
export RUFF_CACHE_DIR="${RUFF_CACHE_DIR:-/tmp/ruff-cache-tdcpass}"

BOOTSTRAP_PYTHON="${PYTHON_BIN:-python3}"
if [ -x "$HOME/.pyenv/versions/3.12.9/bin/python3" ]; then
  BOOTSTRAP_PYTHON="$HOME/.pyenv/versions/3.12.9/bin/python3"
fi

"$BOOTSTRAP_PYTHON" -m venv "$UV_PROJECT_ENVIRONMENT"
"$UV_PROJECT_ENVIRONMENT/bin/pip" install -e '.[dev]'
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass doctor
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass pipeline run
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m tdcpass demo
"$UV_PROJECT_ENVIRONMENT/bin/python" -B -m pytest -q
