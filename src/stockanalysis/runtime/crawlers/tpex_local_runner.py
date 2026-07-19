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
import re
from urllib.parse import unquote, urlparse
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
    def __init__(self, port=9222, user_data_dir=None, proxy=""):
        self.port = port
        if user_data_dir is None:
            user_data_dir = os.path.join(tempfile.gettempdir(), "g4f_chrome_profile_light")
        self.user_data_dir = user_data_dir
        self.process = None
        self.ws = None
        self.id_counter = 0
        self.proxy = proxy
        self.proxy_extension_dir = None

    def configure_proxy(self):
        if not self.proxy:
            return None
        parsed = urlparse(self.proxy)
        if not parsed.hostname:
            raise ValueError("Proxy must include a hostname")
        proxy_server = f"{parsed.scheme}://{parsed.hostname}:{parsed.port or 80}"
        if not parsed.username:
            return proxy_server

        self.proxy_extension_dir = tempfile.mkdtemp(prefix="tpex_proxy_auth_")
        manifest = {
            "manifest_version": 3,
            "name": "TPEX Proxy Authentication",
            "version": "1.0",
            "permissions": ["webRequest", "webRequestAuthProvider"],
            "host_permissions": ["<all_urls>"],
            "background": {"service_worker": "background.js"},
        }
        credentials = {
            "username": unquote(parsed.username),
            "password": unquote(parsed.password or ""),
        }
        with open(os.path.join(self.proxy_extension_dir, "manifest.json"), "w", encoding="utf-8") as file:
            json.dump(manifest, file)
        with open(os.path.join(self.proxy_extension_dir, "background.js"), "w", encoding="utf-8") as file:
            file.write(
                "const credentials = " + json.dumps(credentials) + ";\n"
                "chrome.webRequest.onAuthRequired.addListener(\n"
                "  (_details, callback) => callback({authCredentials: credentials}),\n"
                "  {urls: ['<all_urls>']},\n"
                "  ['asyncBlocking']\n"
                ");\n"
            )
        return proxy_server

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
        proxy_server = self.configure_proxy()
        if proxy_server:
            cmd.append(f"--proxy-server={proxy_server}")
            if self.proxy_extension_dir:
                cmd.extend([
                    f"--disable-extensions-except={self.proxy_extension_dir}",
                    f"--load-extension={self.proxy_extension_dir}",
                ])

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
        if self.proxy_extension_dir:
            shutil.rmtree(self.proxy_extension_dir, ignore_errors=True)
            self.proxy_extension_dir = None

def find_free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(('', 0))
        return s.getsockname()[1]

class BrowserManager:
    def __init__(self, proxy=""):
        self.client = None
        self.port = find_free_port()
        self.user_data_dir = os.path.join(tempfile.gettempdir(), f"g4f_chrome_profile_light_{self.port}")
        self.proxy = proxy

    def get_token(self, target_url):
        if not self.client:
            self.client = Turnstile(
                port=self.port,
                user_data_dir=self.user_data_dir,
                proxy=self.proxy,
            )
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

def crawl_stock_data(stock, token, output_dir, data_date=None, proxy=""):
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
        response = requests.post(
            url,
            data=payload,
            headers=headers,
            proxies={"http": proxy, "https": proxy} if proxy else None,
            timeout=30,
            verify=False,
        )
        response.raise_for_status()
        data = response.json()
        tables = data.get('tables')
        if not isinstance(tables, list) or len(tables) <= 1:
            preview = response.text[:500].replace("\n", " ")
            return False, (
                f"invalid tables structure: table_count={len(tables) if isinstance(tables, list) else 'missing'}, "
                f"status={response.status_code}, content_type={response.headers.get('content-type')}, "
                f"body={preview!r}"
            )

        table = tables[1]
        rows = table.get('data', [])
        fields = table.get('fields', [])
        if not rows:
            return False, f"empty broker table: table_count={len(tables)}, fields={fields}"

        pdf = pd.DataFrame(rows, columns=fields)
        report_date = data_date or datetime.now()
        pdf['日期'] = report_date.strftime('%Y/%m/%d')
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

def parse_runtime_args():
    """Parse local flags or the symbol batch format used by the Cloud Run trigger."""
    limit = None
    target_date = datetime.now()
    symbols = None
    raw_args = sys.argv[1:]

    if raw_args and not raw_args[0].startswith("--"):
        if re.fullmatch(r"\d{8}", raw_args[-1]):
            target_date = datetime.strptime(raw_args.pop(), "%Y%m%d")
        symbols_text = ",".join(raw_args).strip().strip("[]")
        symbols = [
            symbol.strip().strip("'\"")
            for symbol in symbols_text.split(",")
            if symbol.strip().strip("'\"")
        ]
        return target_date, limit, symbols

    for idx, arg in enumerate(raw_args):
        if arg.startswith('--limit='):
            limit = int(arg.split('=', 1)[1])
        elif arg == '--limit' and idx + 1 < len(raw_args):
            limit = int(raw_args[idx + 1])
        elif arg.startswith('--date='):
            target_date = datetime.strptime(arg.split('=', 1)[1], '%Y%m%d')
        elif arg == '--date' and idx + 1 < len(raw_args):
            target_date = datetime.strptime(raw_args[idx + 1], '%Y%m%d')

    return target_date, limit, symbols

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    from google.cloud import storage

    client = storage.Client()
    blob = client.bucket(bucket_name).blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    logging.info("Uploaded to gs://%s/%s", bucket_name, destination_blob_name)

def load_proxies():
    raw = os.getenv("TPEX_PROXIES") or os.getenv("PROXY", "")
    proxies = [line.strip() for line in raw.splitlines() if line.strip()]
    return proxies or [""]

def main():
    try:
        target_date, limit, symbols = parse_runtime_args()
    except ValueError:
        logging.error("Date must use YYYYMMDD format, for example 20260717")
        return 2
    data_dt = target_date.strftime('%Y%m%d')
    bucket_name = os.getenv("STOCK_CRAWLER_BUCKET")
    if bucket_name:
        from google.cloud import firestore

        fs_client = firestore.Client()
    else:
        fs_client = None

    # Setup local output directory
    project_root = os.path.abspath(os.path.dirname(__file__))
    output_root = os.getenv(
        "TPEX_LOCAL_OUTPUT_DIR",
        os.path.join(project_root, "outputs", "tmp", "tpex_bs_report"),
    )
    output_dir = os.path.join(output_root, data_dt)
    os.makedirs(output_dir, exist_ok=True)
    logging.info(f"Local CSV files will be saved to: {output_dir}")

    try:
        traded = load_traded_symbols(target_date)
    except Exception as e:
        logging.error(f"Failed to fetch TPEX OHLC data for {data_dt}: {e}")
        return 1

    if symbols is None:
        symbols = traded
    else:
        logging.info("Received %s symbols from Cloud Run trigger: %s", len(symbols), symbols)
        traded_set = set(traded)
        excluded = [symbol for symbol in symbols if symbol not in traded_set]
        symbols = [symbol for symbol in symbols if symbol in traded_set]
        for symbol in excluded:
            logging.warning("Symbol %s is not in the traded OHLC scope, skipping.", symbol)
            if fs_client:
                fs_client.collection(f"tpex_crawl_status_{data_dt}").document(symbol).delete()

    total_symbols = len(symbols)
    logging.info(
        f"Retrieved {total_symbols} symbols with trading volume from TPEX OHLC data for {data_dt}"
    )

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

    proxies = load_proxies()
    logging.info("Loaded %s proxy endpoint(s)", len([proxy for proxy in proxies if proxy]))
    browser = BrowserManager()
    target_url = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"
    success_count = 0
    failure_count = 0
    try:
        for idx, symbol in enumerate(symbols, 1):
            doc_ref = None
            if fs_client:
                try:
                    doc_ref = fs_client.collection(f"tpex_crawl_status_{data_dt}").document(symbol)
                    if not doc_ref.get().exists:
                        logging.warning("Symbol %s not found in Firestore, skipping.", symbol)
                        continue
                except Exception as e:
                    failure_count += 1
                    logging.error("Failed to access Firestore for symbol %s: %s", symbol, e)
                    continue

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
                proxy = proxies[(idx + retries - 2) % len(proxies)]
                browser.close()
                browser = BrowserManager(proxy=proxy)
                logging.info(
                    "Using proxy %s for symbol %s attempt %s/3",
                    urlparse(proxy).hostname if proxy else "direct",
                    symbol,
                    retries,
                )
                token = browser.get_token(target_url)
                if not token:
                    last_reason = "Turnstile token was empty"
                    logging.error(
                        f"Failed to obtain Turnstile token for symbol {symbol} "
                        f"(attempt {retries}/3): {last_reason}"
                    )
                    continue

                success, last_reason = crawl_stock_data(
                    symbol, token, output_dir, data_date=target_date, proxy=proxy
                )
                if not success:
                    logging.warning(
                        f"Crawl failed for symbol {symbol} (attempt {retries}/3): "
                        f"{last_reason}"
                    )
                    time.sleep(1)

            if success:
                try:
                    if bucket_name:
                        upload_to_gcs(
                            bucket_name,
                            output_path,
                            f"bs_report/tpex/{data_dt}/{symbol}.csv",
                        )
                    if doc_ref:
                        doc_ref.delete()
                        logging.info("Deleted Firestore document for symbol: %s", symbol)
                    success_count += 1
                except Exception as e:
                    failure_count += 1
                    logging.error("Failed to finalize symbol %s: %s", symbol, e)
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
    return 1 if failure_count else 0

if __name__ == "__main__":
    raise SystemExit(main())
