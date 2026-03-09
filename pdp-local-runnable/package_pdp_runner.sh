#!/usr/bin/env bash
set -euo pipefail

RUN_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
RUN_PARENT="$(cd "$RUN_DIR/.." && pwd)"
RUN_BASENAME="$(basename "$RUN_DIR")"
DIST_BIN="$RUN_DIR/pdp-local-runner"
ZIP_FILE="$RUN_PARENT/${RUN_BASENAME}-share.zip"

if [[ ! -x "$DIST_BIN" ]]; then
  echo "Error: standalone binary missing: $DIST_BIN"
  echo "Build first with: $RUN_DIR/build_pdp_executable.sh"
  exit 1
fi

# Ensure expected local folders exist.
mkdir -p \
  "$RUN_DIR/pdp-input" \
  "$RUN_DIR/pdp-crawl-input" \
  "$RUN_DIR/pdp-masters" \
  "$RUN_DIR/pdp-run-output"

rm -f "$ZIP_FILE"
(cd "$RUN_PARENT" && zip -r "$(basename "$ZIP_FILE")" "$RUN_BASENAME" >/dev/null)

echo "Package ready:"
echo "  $ZIP_FILE"
