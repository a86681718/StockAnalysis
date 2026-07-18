#!/bin/sh
set -eu

Xvfb :99 -screen 0 1280x800x24 >/tmp/xvfb.log 2>&1 &
export DISPLAY=:99

exec python /app/src/stockanalysis/runtime/crawlers/tpex_local_runner.py "$@"
