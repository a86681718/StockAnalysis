count=$(ps aux | grep BSReportCrawler | wc -l)
echo $count
if [ "$count" = "1" ]
then
    echo "TWSE is not running."
    sudo shutdown -P now
else
    echo "TWSE is running"
fi
