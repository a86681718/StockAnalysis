import os
import sys
import tempfile
import json
import base64
import shutil
import time
import logging
import requests
import urllib3
import pandas as pd
from datetime import datetime
import platform
import subprocess
import socket
import urllib.request
import urllib.error
from urllib.parse import unquote, urlparse
from itertools import cycle
import re
from google.cloud import firestore, storage
from src.stockanalysis.config import ensure_dir, resolve_output
from websocket import create_connection

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

BUCKET_NAME = os.environ.get('STOCK_CRAWLER_BUCKET')


def load_proxies() -> list[str]:
    raw_proxies = os.environ.get("TPEX_PROXIES") or os.environ.get("PROXY", "")
    proxies = [line.strip() for line in raw_proxies.splitlines() if line.strip()]
    for proxy in proxies:
        parsed = urlparse(proxy)
        if parsed.scheme not in {"http", "https", "socks4", "socks5"} or not parsed.hostname:
            raise ValueError(
                "Each TPEX proxy must be a URL such as http://user:password@host:port."
            )
    return [""] + proxies


def proxy_label(proxy: str) -> str:
    if not proxy:
        return "direct"
    parsed = urlparse(proxy)
    return f"{parsed.hostname}:{parsed.port or 80}"

def find_chrome_path():
    """Search for Google Chrome, Chromium, or Microsoft Edge binary depending on OS."""
    # First, search using shutil.which
    for name in ['google-chrome', 'google-chrome-stable', 'chromium', 'chromium-browser', 'chrome', 'microsoft-edge', 'microsoft-edge-stable']:
        path = shutil.which(name)
        if path:
            return path

    # System default paths
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

        # Default fallback to a persistent directory in temp folder
        if user_data_dir is None:
            user_data_dir = os.path.join(tempfile.gettempdir(), "g4f_chrome_profile_light")

        self.user_data_dir = user_data_dir
        self.process = None
        self.ws = None
        self.id_counter = 0
        self.proxy_extension_dir = None

    def configure_proxy(self, proxy: str) -> str:
        """Return Chrome's proxy URL and configure authentication if needed."""
        parsed = urlparse(proxy)
        if not parsed.hostname:
            raise ValueError("PROXY must include a hostname.")

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
        with open(os.path.join(self.proxy_extension_dir, "manifest.json"), "w") as file:
            json.dump(manifest, file)
        with open(os.path.join(self.proxy_extension_dir, "background.js"), "w") as file:
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
        """Launch Chrome with CDP remote debugging port."""
        chrome_path = find_chrome_path()
        if not chrome_path:
            raise RuntimeError("Google Chrome / Chromium executable not found.")

        logging.info(f"[Turnstile] Launching Chrome/Edge: {chrome_path} on port {self.port}")

        # Create an isolated profile directory
        os.makedirs(self.user_data_dir, exist_ok=True)

        # Keep Crashpad and Chromium's cache in the writable, isolated profile.
        # Cloud Run containers execute as a non-root user, while the base image
        # can contain root-owned browser configuration directories.
        chrome_env = os.environ.copy()
        chrome_env["XDG_CONFIG_HOME"] = os.path.join(self.user_data_dir, "config")
        chrome_env["XDG_CACHE_HOME"] = os.path.join(self.user_data_dir, "cache")
        os.makedirs(chrome_env["XDG_CONFIG_HOME"], exist_ok=True)
        os.makedirs(chrome_env["XDG_CACHE_HOME"], exist_ok=True)

        # Launch arguments matching DrissionPage to minimize detection
        cmd = [
            chrome_path,
            f"--remote-debugging-port={self.port}",
            f"--user-data-dir={self.user_data_dir}",
            # "--window-position=-2000,-2000",
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
            "--use-gl=disabled",
            "--disable-dev-shm-usage"
        ]

        proxy = os.environ.get("PROXY")
        if proxy:
            proxy_server = self.configure_proxy(proxy)
            logging.info("[Turnstile] Using proxy server: %s", proxy_label(proxy))
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

        self.connect()  # Wait for Chrome to be ready and connect via WebSocket

    def connect(self):
        # Wait for CDP port readiness and retrieve the WebSocket URL
        ws_url = None
        for i in range(40):  # Up to 20 seconds
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

        # Enable necessary CDP domains
        self.call_cdp("Page.enable")
        self.call_cdp("DOM.enable")
        self.call_cdp("Runtime.enable")
        self.call_cdp("Emulation.setFocusEmulationEnabled", enabled=True)
        hardware_concurrency = os.environ.get("TURNSTILE_HARDWARE_CONCURRENCY")
        if hardware_concurrency:
            try:
                hardware_concurrency = int(hardware_concurrency)
            except ValueError as exc:
                raise ValueError("TURNSTILE_HARDWARE_CONCURRENCY must be an integer.") from exc
            self.call_cdp(
                "Emulation.setHardwareConcurrencyOverride",
                hardwareConcurrency=hardware_concurrency,
            )

    def call_cdp(self, method, **params):
        """Call CDP method and wait for response."""
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
        """Execute JS code on the page and return the result."""
        res = self.call_cdp("Runtime.evaluate", expression=expression, returnByValue=True)
        return res.get("result", {}).get("value")

    def capture_turnstile_diagnostics(self, label: str) -> None:
        """Log challenge state and optionally upload a screenshot to GCS."""
        try:
            details = self.evaluate_js("""
                JSON.stringify({
                    title: document.title,
                    url: location.href,
                    readyState: document.readyState,
                    turnstileResponsePresent: Boolean(document.querySelector('[name=cf-turnstile-response]')),
                    turnstileResponseLength: (document.querySelector('[name=cf-turnstile-response]') || {}).value?.length || 0,
                    turnstileFrames: Array.from(document.querySelectorAll('iframe')).map(frame => frame.src),
                    webdriver: navigator.webdriver,
                    hardwareConcurrency: navigator.hardwareConcurrency,
                    pluginsLength: navigator.plugins.length,
                    userAgent: navigator.userAgent
                })
            """)
            logging.info("[Turnstile] Challenge diagnostics: %s", details)

            if os.environ.get("TURNSTILE_DIAGNOSTICS") != "1" or not BUCKET_NAME:
                return
            screenshot = self.call_cdp("Page.captureScreenshot", format="png").get("data")
            if not screenshot:
                return
            timestamp = datetime.now().strftime("%Y%m%dT%H%M%S")
            filename = f"tpex_turnstile_{label}_{timestamp}.png"
            screenshot_path = os.path.join(tempfile.gettempdir(), filename)
            with open(screenshot_path, "wb") as image_file:
                image_file.write(base64.b64decode(screenshot))
            blob_name = f"debug/tpex_turnstile/{filename}"
            upload_to_gcs(BUCKET_NAME, screenshot_path, blob_name)
            logging.info("[Turnstile] Uploaded screenshot to gs://%s/%s", BUCKET_NAME, blob_name)
        except Exception as exc:
            logging.exception("[Turnstile] Failed to capture diagnostics: %s", exc)

    def get_token(self, target_url: str) -> str:
        """Retrieve Turnstile token for the target URL."""
        if not self.ws:
            self.start_chrome()

        logging.info(f"[Turnstile] Navigating to {target_url}...")
        self.call_cdp("Page.navigate", url=target_url)

        # Give some time to load
        time.sleep(3.0)

        # If target URL is deepinfra, we perform the deepinfra-specific textarea typing interaction
        if "deepinfra.com" in target_url:
            # Inject completions request blocker ONLY in main frame
            fetch_blocker_js = """
            const origFetch = window.fetch;
            window.fetch = async function(...args) {
                let url = args[0];
                if (typeof url === 'string' && url.includes('/chat/completions')) {
                    return new Response('{}', {status: 200});
                }
                return origFetch.apply(this, args);
            };
            """
            self.evaluate_js(fetch_blocker_js)

            # Try to click "Accept" on cookies consent popup if present
            self.evaluate_js("""
            (() => {
                const btn = Array.from(document.querySelectorAll('button')).find(b => b.textContent.trim() === 'Accept');
                if (btn) btn.click();
            })()
            """)

            # Wait for textarea readiness, focus and input text
            logging.info("[Turnstile] Waiting for active textarea...")
            text_entered = False
            for _ in range(40):  # Up to 20 seconds
                try:
                    ready = self.evaluate_js("""
                    (() => {
                        const ta = document.querySelector('textarea');
                        if (!ta) return 'no_textarea';
                        if (ta.disabled) return 'disabled';
                        ta.click();
                        ta.focus();
                        ta.scrollIntoView({ block: 'center' });
                        return 'ready';
                    })()
                    """)

                    if ready == 'ready':
                        logging.info("[Turnstile] Textarea found, focusing and entering text...")

                        # Retrieve textarea nodeId for native focusing
                        doc = self.call_cdp('DOM.getDocument')
                        root_id = doc['root']['nodeId']
                        textarea = self.call_cdp('DOM.querySelector', nodeId=root_id, selector='textarea')

                        # Native focus via CDP
                        self.call_cdp('DOM.focus', nodeId=textarea['nodeId'])

                        # Enter text via native CDP command
                        self.call_cdp("Input.insertText", text="Test Prompt")

                        # Give React some time to process input before pressing Enter
                        time.sleep(0.5)

                        # Simulate Enter keypress via native CDP events
                        self.call_cdp("Input.dispatchKeyEvent",
                                      type="keyDown",
                                      windowsVirtualKeyCode=13,
                                      key="Enter",
                                      code="Enter",
                                      text="\r",
                                      unmodifiedText="\r")
                        self.call_cdp("Input.dispatchKeyEvent",
                                      type="keyUp",
                                      windowsVirtualKeyCode=13,
                                      key="Enter",
                                      code="Enter",
                                      text="\r",
                                      unmodifiedText="\r")

                        text_entered = True
                        break
                except Exception:
                    pass
                time.sleep(0.5)

            if not text_entered:
                logging.error("[-] Turnstile initiation error: textarea not found or disabled.")
                return ""
        else:
            # General site handling: perform some basic interaction to ensure Turnstile triggers
            logging.info("[Turnstile] Performing page interaction (scrolling)...")
            self.evaluate_js("window.scrollTo(0, 100);")

        # Poll page for Turnstile token presence
        logging.info("[Turnstile] Waiting for Cloudflare Turnstile solve...")
        token_js = "document.querySelector('[name=cf-turnstile-response]') ? document.querySelector('[name=cf-turnstile-response]').value : ''"
        token = ""
        for i in range(120):  # Up to 60 seconds
            try:
                token = self.evaluate_js(token_js)
                if token:
                    logging.info(f"[Turnstile] Token generated on check {i+1}!")
                    break
            except Exception:
                pass
            time.sleep(0.5)

        return token

    def close(self):
        """Close connection and terminate Chrome process."""
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

def get_turnstile_token_sync(target_url: str, diagnostic_label: str) -> str:
    """Get Turnstile token using Turnstile with a fallback to DrissionPage."""
    # 1. Try lightweight CDP client
    try:
        port = find_free_port()
        user_data_dir = os.path.join(tempfile.gettempdir(), f"g4f_chrome_profile_light_{port}")
        client = Turnstile(port=port, user_data_dir=user_data_dir)
        try:
            token = client.get_token(target_url)
            if token:
                return token
            client.capture_turnstile_diagnostics(diagnostic_label)
            logging.error("[Turnstile] Failed to obtain token. Trying fallback option (DrissionPage)...")
        finally:
            client.close()
    except Exception as e:
        logging.error(f"[Turnstile] CDP Error: {e}. Trying fallback option (DrissionPage)...")

    # 2. Fallback option: try original DrissionPage if installed
    proxy_extension_dir = None
    page = None
    try:
        from DrissionPage import ChromiumPage, ChromiumOptions
        co = ChromiumOptions()
        proxy = os.environ.get("PROXY")
        if proxy:
            proxy_client = Turnstile()
            proxy_server = proxy_client.configure_proxy(proxy)
            proxy_extension_dir = proxy_client.proxy_extension_dir
            co.set_proxy(proxy_server)
            if proxy_extension_dir:
                co.add_extension(proxy_extension_dir)
        co.auto_port()
        chrome_path = find_chrome_path()
        if chrome_path:
            co.set_browser_path(chrome_path)
        # co.set_argument('--window-position=-2000,-2000') # keep visible
        co.set_argument('--window-size=1024,768')
        co.set_argument('--log-level=3')
        page = ChromiumPage(co)
        try:
            page.get(target_url)

            if "deepinfra.com" in target_url:
                # Block completions requests
                js_block_fetch = """
                const origFetch = window.fetch;
                window.fetch = async function(...args) {
                    let url = args[0];
                    if (typeof url === 'string' && url.includes('/chat/completions')) {
                        return new Response('{}', {status: 200});
                    }
                    return origFetch.apply(this, args);
                };
                """
                page.run_js(js_block_fetch)

                textarea = page.ele('tag:textarea', timeout=15)
                if textarea:
                    textarea.input('Test Prompt')
                    textarea.input('\n')
            else:
                page.scroll.down(100)

            token_input = page.ele('@name=cf-turnstile-response', timeout=20)
            if token_input:
                for _ in range(40):
                    token = token_input.attr('value')
                    if token:
                        return token
                    time.sleep(0.5)
        finally:
            page.quit()
            page = None
    except Exception as e:
        logging.error(f"[Turnstile] Fallback DrissionPage method error: {e}")
    finally:
        if page is not None:
            try:
                page.quit()
            except Exception:
                pass
        if proxy_extension_dir:
            shutil.rmtree(proxy_extension_dir, ignore_errors=True)

    return ""

def crawl_stock_data(stock, token, output_path, proxy, data_dt):
    """Crawl stock data and save it as a CSV file."""
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
        if response.ok:
            body = response.json()
            tables = body.get("tables", [])
            if len(tables) < 2:
                logging.error("TPEX returned no broker table for stock %s", stock)
                return False
            table = tables[1]
            pdf = pd.DataFrame(table['data'], columns=table['fields'])
            pdf['日期'] = datetime.strptime(data_dt, '%Y%m%d').strftime('%Y/%m/%d')
            pdf['券商'] = pdf['券商'].str.split().str[0]
            pdf.drop(columns=['序號'], inplace=True)
            pdf.to_csv(output_path, encoding='utf8', index=False)
            logging.info(f"Successfully crawled and saved data for stock {stock}")
            return True
    except (requests.RequestException, ValueError, KeyError, IndexError) as e:
        logging.error(f"Failed to crawl data for stock {stock}: {e}")
        return False

def upload_to_gcs(bucket_name, source_file_path, destination_blob_name):
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(source_file_path)
    print(f"Uploaded to gs://{bucket_name}/{destination_blob_name}")

def main():
    # Set environment variables
    os.environ['DISPLAY'] = ':99'

    # Parse the arguments
    logging.info(f"sys.argv: {sys.argv}")
    if len(sys.argv) < 2:
        logging.error("Missing symbols argument. Expected e.g. ['8069'] 20260711.")
        return 2

    raw_args = sys.argv[1:]
    if re.fullmatch(r"\d{8}", raw_args[-1].strip()):
        data_dt = raw_args.pop().strip()
    else:
        data_dt = datetime.now().strftime('%Y%m%d')
    try:
        datetime.strptime(data_dt, '%Y%m%d')
    except ValueError:
        logging.error("Invalid data date %r. Expected YYYYMMDD.", data_dt)
        return 2

    symbols_text = ",".join(raw_args).strip().strip("[]")
    symbols = [
        symbol.strip().strip("'\"")
        for symbol in symbols_text.split(",")
        if symbol.strip().strip("'\"")
    ]
    if not symbols:
        logging.error("No valid stock symbols were provided.")
        return 2
    logging.info(f"Received symbols: {symbols}")
    logging.info(f"Data date: {data_dt}")

    output_dir = ensure_dir(resolve_output("tmp", "tpex_bs_report"))
    try:
        proxies = load_proxies()
    except ValueError as exc:
        logging.error(exc)
        return 2
    proxy_cycle = cycle(enumerate(proxies, start=1))

    fs_client = firestore.Client()
    # Process each symbol
    failures = 0
    for symbol in symbols:
        logging.info(f"Starting to crawl stock {symbol}")

        # Check if the symbol document exists in Firestore
        try:
            collection_name = f"tpex_crawl_status_{data_dt}"
            doc_ref = fs_client.collection(collection_name).document(symbol)
            doc = doc_ref.get()
            if not doc.exists:
                logging.warning(f"Symbol {symbol} not found in Firestore, skipping.")
                continue
        except Exception as e:
            logging.error(f"Failed to access Firestore for symbol {symbol}: {e}")
            failures += 1
            continue
        output_path = str(output_dir / f"{symbol}.csv")
        success = False
        retries = 0
        while retries < 3 and not success:
            retries += 1
            proxy_index, proxy = next(proxy_cycle)
            if proxy:
                os.environ["PROXY"] = proxy
            else:
                os.environ.pop("PROXY", None)
            logging.info(
                "Retrieving Turnstile token for symbol %s, attempt %s/3 via proxy %s/%s (%s)...",
                symbol,
                retries,
                proxy_index,
                len(proxies),
                proxy_label(proxy),
            )
            try:
                token = get_turnstile_token_sync(
                    "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html",
                    f"{symbol}_attempt_{retries}",
                )
                if not token:
                    logging.error(f"Unable to retrieve Turnstile token, skipping stock {symbol}")
                    continue

                # Crawl data
                success = crawl_stock_data(symbol, token, output_path, proxy, data_dt)
            except Exception as e:
                logging.error(f"Error during crawl attempt {retries}: {e}")

        if success:
            if os.path.exists(output_path):
                # Upload to Google Cloud Storage
                gcs_blob_name = f'bs_report/tpex/{data_dt}/{symbol}.csv'
                upload_to_gcs(BUCKET_NAME, output_path, gcs_blob_name)

            # Delete the symbol document from Firestore
            try:
                collection_name = f"tpex_crawl_status_{data_dt}"

                doc_ref = fs_client.collection(collection_name).document(symbol)
                doc_ref.delete()
                logging.info(f"Deleted Firestore document for symbol: {symbol}")
            except Exception as e:
                logging.error(f"Failed to delete Firestore document for symbol {symbol}: {e}")
        else:
            logging.error(f"Failed to crawl data for stock {symbol} after 3 retries")
            failures += 1

    return 1 if failures else 0

if __name__ == "__main__":
    if not BUCKET_NAME:
        logging.error("Missing STOCK_CRAWLER_BUCKET environment variable.")
        sys.exit(1)
    sys.exit(main())
