import os
import sys
import tempfile
import json
import shutil
import time
import logging
import requests
import urllib3
import platform
import subprocess
import socket
import urllib.request
import urllib.error
from datetime import datetime
from websocket import create_connection

import pandas as pd

SRC_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../.."))
if SRC_ROOT not in sys.path:
    sys.path.insert(0, SRC_ROOT)

from stockanalysis.runtime.crawlers.tpex_daily_ohlc import fetch_tpex_daily

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging configuration
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout)
    ]
)

def find_chrome_path():
    """Search for Google Chrome, Chromium, or Microsoft Edge binary depending on OS."""
    for name in ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser', 'chrome', 'microsoft-edge', 'microsoft-edge-stable']:
        path = shutil.which(name)
        if path:
            return path

    sys_name = platform.system().lower()
    if sys_name == 'linux':
        for path in ['/usr/bin/google-chrome', '/opt/google/chrome/google-chrome', '/usr/bin/chromium-browser']:
            if os.path.exists(path):
                return path
    elif sys_name in ('macos', 'darwin'):
        paths = [
            '/Applications/Google Chrome.app/Contents/MacOS/Google Chrome',
            '/Applications/Microsoft Edge.app/Contents/MacOS/Microsoft Edge',
            '/Applications/Chromium.app/Contents/MacOS/Chromium'
        ]
        for path in paths:
            if os.path.exists(path):
                return path
    elif sys_name == 'windows':
        paths = [
            r"C:\Program Files\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Google\Chrome\Application\chrome.exe",
            r"C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe",
        ]
        for path in paths:
            if os.path.exists(path):
                return path
    return None

class Turnstile:
    def __init__(self, port=9222, user_data_dir=None):
        self.port = port
        if user_data_dir is None:
            user_data_dir = os.path.join(tempfile.gettempdir(), "g4f_chrome_profile_light")
        self.user_data_dir = user_data_dir
        self.process = None
        self.ws = None
        self.id_counter = 0

    def start_chrome(self):
        chrome_path = find_chrome_path()
        if not chrome_path:
            raise RuntimeError("Google Chrome / Chromium executable not found.")

        logging.info(f"[Turnstile] Launching Chrome/Edge: {chrome_path} on port {self.port}")
        os.makedirs(self.user_data_dir, exist_ok=True)

        chrome_env = os.environ.copy()
        if platform.system().lower() == 'linux':
            chrome_env["XDG_CONFIG_HOME"] = os.path.join(self.user_data_dir, "config")
            chrome_env["XDG_CACHE_HOME"] = os.path.join(self.user_data_dir, "cache")
            os.makedirs(chrome_env["XDG_CONFIG_HOME"], exist_ok=True)
            os.makedirs(chrome_env["XDG_CACHE_HOME"], exist_ok=True)

        cmd = [
            chrome_path,
            f"--remote-debugging-port={self.port}",
            f"--user-data-dir={self.user_data_dir}",
            "--window-size=1024,768",
            "--no-default-browser-check",
            "--disable-suggestions-ui",
            "--no-first-run",
            "--disable-infobars",
            "--disable-popup-blocking",
            "--hide-crash-restore-bubble",
            "--disable-features=PrivacySandboxSettings4",
            "--remote-allow-origins=*",
            "--no-sandbox",
            "--disable-gpu",
            "--disable-dev-shm-usage"
        ]

        self.process = subprocess.Popen(
            cmd,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            env=chrome_env,
        )
        self.connect()

    def connect(self):
        ws_url = None
        for i in range(40):
            time.sleep(0.5)
            try:
                with urllib.request.urlopen(f"http://127.0.0.1:{self.port}/json", timeout=2) as req:
                    targets = json.loads(req.read().decode('utf-8'))
                    for target in targets:
                        if target.get('type') in ('page', 'webview'):
                            ws_url = target.get('webSocketDebuggerUrl')
                            break
                    if ws_url:
                        break
            except (urllib.error.URLError, ConnectionResetError, ConnectionRefusedError):
                pass

        if not ws_url:
            self.close()
            raise RuntimeError(f"Failed to connect to Chrome debugging port 127.0.0.1:{self.port}")

        logging.info(f"[Turnstile] Connected to CDP WebSocket: {ws_url}")
        self.ws = create_connection(ws_url, timeout=15)
        self.call_cdp("Page.enable")
        self.call_cdp("DOM.enable")
        self.call_cdp("Runtime.enable")
        self.call_cdp("Emulation.setFocusEmulationEnabled", enabled=True)

    def call_cdp(self, method, **params):
        self.id_counter += 1
        payload = {
            "id": self.id_counter,
            "method": method,
            "params": params
        }
        self.ws.send(json.dumps(payload))
        while True:
            response = json.loads(self.ws.recv())
            if response.get("id") == self.id_counter:
                if "error" in response:
                    raise RuntimeError(f"CDP Error calling {method}: {response['error']}")
                return response.get("result", {})

    def evaluate_js(self, expression):
        res = self.call_cdp("Runtime.evaluate", expression=expression, returnByValue=True)
        return res.get("result", {}).get("value")

    def close(self):
        if self.ws:
            try:
                self.ws.close()
            except Exception:
                pass
            self.ws = None
        if self.process:
            try:
                self.process.terminate()
                self.process.wait(timeout=5)
            except Exception:
                try:
                    self.process.kill()
                except Exception:
                    pass
            self.process = None

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

class BrowserManager:
    def __init__(self):
        self.client = None
        self.port = find_free_port()
        self.user_data_dir = os.path.join(tempfile.gettempdir(), f"g4f_chrome_profile_light_{self.port}")

    def get_token(self, target_url):
        if not self.client:
            self.client = Turnstile(port=self.port, user_data_dir=self.user_data_dir)
            self.client.start_chrome()
            logging.info(f"[BrowserManager] Initial load of {target_url}...")
            self.client.call_cdp("Page.navigate", url=target_url)
        else:
            logging.info("[BrowserManager] Reloading page to generate new Turnstile token...")
            self.client.call_cdp("Page.reload")

        # Give some time to load
        time.sleep(2.5)

        try:
            self.client.evaluate_js("window.scrollTo(0, 100);")
        except Exception:
            pass

        token_js = "document.querySelector('[name=cf-turnstile-response]') ? document.querySelector('[name=cf-turnstile-response]').value : ''"
        token = ""
        for i in range(40):  # Up to 20 seconds
            try:
                token = self.client.evaluate_js(token_js)
                if token:
                    logging.info(f"[BrowserManager] Turnstile Token obtained on check {i+1}")
                    break
            except Exception:
                pass
            time.sleep(0.5)
        return token

    def close(self):
        if self.client:
            self.client.close()
            self.client = None

def crawl_stock_data(stock, token, output_dir):
    """Crawl stock data from TPEX and return success plus a diagnostic reason."""
    url = "https://www.tpex.org.tw/www/zh-tw/afterTrading/brokerBS"
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.tpex.org.tw",
        "Referer": "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html",
        "User-Agent": "Mozilla/5.0"
    }
    payload = {
        "cf-turnstile-response": token,
        "code": stock
    }
    try:
        response = requests.post(url, data=payload, headers=headers, timeout=15, verify=False)
        response.raise_for_status()
        data = response.json()
        tables = data.get('tables')
        if not isinstance(tables, list) or len(tables) <= 1:
            return False, f"invalid tables structure: table_count={len(tables) if isinstance(tables, list) else 'missing'}"

        table = tables[1]
        rows = table.get('data', [])
        fields = table.get('fields', [])
        if not rows:
            return False, f"empty broker table: table_count={len(tables)}, fields={fields}"

        pdf = pd.DataFrame(rows, columns=fields)
        pdf['日期'] = datetime.now().strftime('%Y/%m/%d')
        pdf['券商'] = pdf['券商'].str.split().str[0]
        pdf.drop(columns=['序號'], inplace=True)

        output_path = os.path.join(output_dir, f"{stock}.csv")
        pdf.to_csv(output_path, encoding='utf8', index=False)
        logging.info(f"[Crawler] Successfully saved CSV for stock {stock} ({len(rows)} records) to {output_path}")
        return True, f"saved_rows={len(rows)}"
    except ValueError as e:
        reason = f"invalid JSON response: {e}"
        logging.error(f"[Crawler] Stock {stock}: {reason}")
        return False, reason
    except Exception as e:
        reason = f"{type(e).__name__}: {e}"
        logging.error(f"[Crawler] Stock {stock}: request or save failed: {reason}")
        return False, reason

def traded_symbols(daily):
    traded_volume = pd.to_numeric(
        daily["成交股數"].astype(str).str.replace(",", "", regex=False),
        errors="coerce",
    )
    symbols = daily.loc[traded_volume > 0, "代號"].astype(str).str.strip()
    return symbols[symbols.ne("")].drop_duplicates().tolist()

def load_traded_symbols(target_date):
    """Load traded TPEX stocks and call/put warrants from the OHLC API."""
    stocks = fetch_tpex_daily(target_date, security_type="EW")
    warrants = fetch_tpex_daily(target_date, security_type="WW")

    if not stocks.empty and {"代號", "成交股數"}.issubset(stocks.columns):
        stocks = stocks[stocks["代號"].astype(str).str.strip().str.len() == 4]
        stock_symbols = traded_symbols(stocks)
    else:
        stock_symbols = []

    if not warrants.empty and {"代號", "名稱", "成交股數"}.issubset(warrants.columns):
        warrant_name = warrants["名稱"].astype(str).str.strip()
        warrants = warrants[warrant_name.str.contains("購|售", regex=True)]
        warrant_symbols = traded_symbols(warrants)
    else:
        warrant_symbols = []

    return stock_symbols + warrant_symbols

def main():
    limit = None
    # Parse simple arguments
    for idx, arg in enumerate(sys.argv):
        if arg.startswith('--limit='):
            try:
                limit = int(arg.split('=')[1])
            except ValueError:
                pass
        elif arg == '--limit' and idx + 1 < len(sys.argv):
            try:
                limit = int(sys.argv[idx + 1])
            except ValueError:
                pass
    data_dt = datetime.now().strftime('%Y%m%d')

    # Setup local output directory
    project_root = os.path.abspath(os.path.dirname(__file__))
    output_dir = os.path.join(project_root, "outputs", "tmp", "tpex_bs_report", data_dt)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Local CSV files will be saved to: {output_dir}")

    # Retrieve symbols directly from the TPEX OHLC API source.
    try:
        symbols = load_traded_symbols(datetime.now())
    except Exception as e:
        logging.error(f"Failed to fetch today's TPEX OHLC data: {e}")
        sys.exit(1)

    total_symbols = len(symbols)
    logging.info(f"Retrieved {total_symbols} symbols with trading volume from today's TPEX OHLC data")

    # Prioritize 4-digit stock symbols, then others
    four_digits = [s for s in symbols if len(s) == 4]
    others = [s for s in symbols if len(s) != 4]
    symbols = four_digits + others
    logging.info(f"Sorted symbols: {len(four_digits)} 4-digit symbols prioritized, followed by {len(others)} other symbols.")

    if limit:
        symbols = symbols[:limit]
        total_symbols = len(symbols)
        logging.info(f"Limiting execution to the first {limit} symbols: {symbols}")

    if not symbols:
        logging.info("No symbols to process.")
        return

    browser = BrowserManager()
    target_url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"
    success_count = 0
    failure_count = 0
    try:
        for idx, symbol in enumerate(symbols, 1):
            output_path = os.path.join(output_dir, f"{symbol}.csv")
            if os.path.exists(output_path):
                logging.info(f"[{idx}/{total_symbols}] Stock {symbol} CSV already exists, skipping.")
                success_count += 1
                continue

            logging.info(f"[{idx}/{total_symbols}] Processing stock {symbol}...")
            success = False
            retries = 0
            last_reason = "unknown"
            while retries < 3 and not success:
                retries += 1
                token = browser.get_token(target_url)
                if not token:
                    last_reason = "Turnstile token was empty"
                    logging.error(
                        f"Failed to obtain Turnstile token for symbol {symbol} "
                        f"(attempt {retries}/3): {last_reason}"
                    )
                    continue

                success, last_reason = crawl_stock_data(symbol, token, output_dir)
                if not success:
                    logging.warning(
                        f"Crawl failed for symbol {symbol} (attempt {retries}/3): "
                        f"{last_reason}"
                    )
                    time.sleep(1)

            if success:
                success_count += 1
            else:
                failure_count += 1
                logging.error(
                    f"FINAL FAILURE for symbol {symbol} after {retries} attempts: "
                    f"{last_reason}"
                )

            time.sleep(0.5)
    except KeyboardInterrupt:
        logging.info("Process interrupted by user.")
    finally:
        browser.close()

    logging.info(f"Process completed. Success: {success_count}, Failures: {failure_count}")

if __name__ == "__main__":
    main()
