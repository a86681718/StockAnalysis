count=$(ps aux | grep TPEX_Crawler.py | wc -l)
echo $count
if [ "$count" = "1" ]
then
    echo "TPEX is not running."
    /usr/bin/ssh vm-2 "sh change_ip.sh" &
    /usr/bin/sleep 10
    /userap/anaconda3/bin/python3 /userap/BuySellReport/TPEX_Crawler.py >> /userap/BuySellReport/log/tpex.log 2>&1 &
else
    echo "TPEX is running"
fi
