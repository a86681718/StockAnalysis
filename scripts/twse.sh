ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
LOG_DIR="$ROOT_DIR/outputs/logs"
mkdir -p "$LOG_DIR"
count=$(ps aux | grep crawler-twse-bsreport.py | wc -l)
echo $count
if [ "$count" = "1" ]
then
    echo "TWSE is not running."
    python3 "$ROOT_DIR/apps/twse/crawler-twse-bsreport.py" >> "$LOG_DIR/twse.log" 2>&1 &
else
    echo "TWSE is running"
fi
