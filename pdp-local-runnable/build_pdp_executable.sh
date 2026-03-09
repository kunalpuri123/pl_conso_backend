#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$RUN_DIR/.." && pwd)"
VENV_PY="$REPO_DIR/venv/bin/python"
VENV_PIP="$REPO_DIR/venv/bin/pip"
ENTRY_SCRIPT="$REPO_DIR/pdp_check.py"
BUILD_ROOT="$RUN_DIR/.build"
BINARY_PATH="$RUN_DIR/pdp-local-runner"
BINARY_NAME="${1:-pdp-local-runner}"

cd "$REPO_DIR"

if [[ ! -x "$VENV_PY" ]]; then
  echo "Error: missing virtualenv python: $VENV_PY"
  echo "Run:"
  echo "  cd $REPO_DIR"
  echo "  python3 -m venv venv"
  echo "  ./venv/bin/pip install -r requirements.txt"
  exit 1
fi

if [[ ! -f "$ENTRY_SCRIPT" ]]; then
  echo "Error: missing entry script: $ENTRY_SCRIPT"
  exit 1
fi

# Ensure local runtime folders are included in repo/workflow expectations.
mkdir -p \
  "$RUN_DIR/pdp-input" \
  "$RUN_DIR/pdp-crawl-input" \
  "$RUN_DIR/pdp-masters" \
  "$RUN_DIR/pdp-run-output"

if ! "$VENV_PY" -m PyInstaller --version >/dev/null 2>&1; then
  echo "Installing PyInstaller in project venv..."
  "$VENV_PIP" install pyinstaller
fi

rm -rf "$BUILD_ROOT"
rm -f "$BINARY_PATH"

"$VENV_PY" -m PyInstaller \
  --clean \
  --noconfirm \
  --onefile \
  --name "$BINARY_NAME" \
  --distpath "$RUN_DIR" \
  --workpath "$BUILD_ROOT/work" \
  --specpath "$BUILD_ROOT/spec" \
  "$ENTRY_SCRIPT"

echo
echo "Build complete."
echo "Binary path: $RUN_DIR/$BINARY_NAME"
echo
echo "Run locally (no Python needed on target machine):"
echo "  cd $RUN_DIR"
echo "  ./$BINARY_NAME"
