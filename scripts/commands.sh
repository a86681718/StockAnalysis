Xvfb :2 -screen 0 1280x800x16 &
/userap/firefox/firefox --private-window https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.php?l=zh-tw &
xdotool search --onlyvisible firefox
xdotool windowmove 2097154 0 42
xdotool windowsize 2097154 1280 758

google-chrome-stable --no-sandbox --window-size=1280,800 --window-position=0,0 --app https://www.tpex.org.tw/web/stock/aftertrading/broker_trading/brokerBS.p
hp?l=zh-tw --incognito >> /dev/null 2>&1 &
