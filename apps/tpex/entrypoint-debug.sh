#!/bin/bash
set -u

echo "[DEBUG] Debug container received args: $@"
mkdir -p "${TPEX_DEBUG_OUTPUT_DIR:-/tmp/tpex-debug}"

Xvfb :99 -screen 0 1280x800x24 >/tmp/xvfb.log 2>&1 &
export DISPLAY=:99

echo "[DEBUG] uname: $(uname -a)"
echo "[DEBUG] google-chrome: $(google-chrome --version 2>/dev/null || true)"
echo "[DEBUG] chromedriver: $(chromedriver --version 2>/dev/null || true)"
echo "[DEBUG] xvfb log: /tmp/xvfb.log"

python debug_tpex_bs_report.py "$@"
