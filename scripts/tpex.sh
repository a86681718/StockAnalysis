ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/outputs/logs"
mkdir -p "$LOG_DIR"
count=$(ps aux | grep crawler-tpex-bsreport.py | wc -l)
echo $count
if [ "$count" = "1" ]
then
    echo "TPEX is not running."
    /usr/bin/ssh vm-2 "sh change_ip.sh" &
    /usr/bin/sleep 10
    python3 "$ROOT_DIR/apps/tpex/crawler-tpex-bsreport.py" >> "$LOG_DIR/tpex.log" 2>&1 &
else
    echo "TPEX is running"
fi
