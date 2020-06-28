count=$(ps aux | grep BSReportCrawler | wc -l)
echo $count
if [ "$count" = "1" ]
then
    echo "TWSE is not running."
    /userap/anaconda3/bin/python3 /userap/BuySellReport/BSReportCrawler.py >> /userap/BuySellReport/log/bs.log 2>&1 &
else
    echo "TWSE is running"
fi
