/userap/firefox/firefox --private-window https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php?l=zh-tw &
/usr/bin/sleep 3
window_id=$(/usr/bin/xdotool search --onlyvisible --name firefox)
echo $window_id
/usr/bin/xdotool windowmove $window_id  0 42
/usr/bin/xdotool windowsize $window_id  1280 758
/usr/bin/scrot /userap/1.png
