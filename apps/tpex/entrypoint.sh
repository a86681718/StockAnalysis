#!/bin/bash
echo "[DEBUG] Container received args: $@"

# 啟動虛擬顯示器
Xvfb :99 -screen 0 1280x800x24 &
export DISPLAY=:99

# # 模擬聲音卡，避免 fingerprint audio fail
# pulseaudio --start

# # 確認 X 啟動與硬體資訊
# xrandr --current || echo "xrandr not available"
# glxinfo | grep "OpenGL" || echo "No OpenGL info"

# 執行 Python 爬蟲
python crawler-tpex-bsreport.py "$@"
