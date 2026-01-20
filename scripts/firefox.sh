/usr/bin/env firefox --private-window https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php?l=zh-tw &
/usr/bin/sleep 3
window_id=$(/usr/bin/xdotool search --onlyvisible --name firefox)
echo $window_id
/usr/bin/xdotool windowmove $window_id  0 42
/usr/bin/xdotool windowsize $window_id  1280 758
ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
mkdir -p "$ROOT_DIR/outputs/tmp"
/usr/bin/scrot "$ROOT_DIR/outputs/tmp/1.png"
