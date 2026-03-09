#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
BIN_PATH="$ROOT_DIR/pdp-local-runner"

cd "$ROOT_DIR"

# Ensure expected local folders always exist.
mkdir -p \
  "$ROOT_DIR/pdp-input" \
  "$ROOT_DIR/pdp-crawl-input" \
  "$ROOT_DIR/pdp-masters" \
  "$ROOT_DIR/pdp-run-output"

if [[ ! -x "$BIN_PATH" ]]; then
  echo "Error: executable not found: $BIN_PATH"
  echo "Put 'pdp-local-runner' in this folder and run again."
  exit 1
fi

if [[ $# -eq 0 ]]; then
  echo "Running PDP local runner (no Python required)..."
fi

exec "$BIN_PATH" "$@"
