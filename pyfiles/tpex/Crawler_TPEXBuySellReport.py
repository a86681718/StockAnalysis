import os
import sys
import tempfile
import json
import shutil
import time
import logging
import requests
import urllib3
import pandas as pd
from datetime import datetime
from DrissionPage import ChromiumPage, ChromiumOptions
from google.cloud import firestore, storage
# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Logging configuration
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

MANIFEST_CONTENT = {
    "manifest_version": 3,
    "name": "Turnstile Patcher",
    "version": "0.1",
    "content_scripts": [{
        "js": ["./script.js"],
        "matches": ["<all_urls>"],
        "run_at": "document_start",
        "all_frames": True,
        "world": "MAIN"
    }]
}

SCRIPT_CONTENT = """
function getRandomInt(min, max) {
    return Math.floor(Math.random() * (max - min + 1)) + min;
}
let screenX = getRandomInt(800, 1200);
let screenY = getRandomInt(400, 600);
Object.defineProperty(MouseEvent.prototype, 'screenX', { value: screenX });
Object.defineProperty(MouseEvent.prototype, 'screenY', { value: screenY });

Object.defineProperty(navigator, 'webdriver', { get: () => undefined });
Object.defineProperty(navigator, 'languages', { get: () => ['zh-TW', 'zh'] });
Object.defineProperty(navigator, 'platform', { get: () => 'Linux x86_64' });
Object.defineProperty(navigator, 'plugins', { get: () => [1, 2, 3] });
Object.defineProperty(navigator, 'maxTouchPoints', { get: () => 1 });
"""

def _create_extension() -> str:
    """ Create temp extension file"""
    temp_dir = tempfile.mkdtemp(prefix='turnstile_extension_')
    
    try:
        manifest_path = os.path.join(temp_dir, 'manifest.json')
        with open(manifest_path, 'w', encoding='utf-8') as f:
            json.dump(MANIFEST_CONTENT, f, indent=4)
        
        script_path = os.path.join(temp_dir, 'script.js')
        with open(script_path, 'w', encoding='utf-8') as f:
            f.write(SCRIPT_CONTENT.strip())
        logging.debug(f"EXTENSION DIR: {temp_dir}")
        logging.debug(f"manifest.json:\n{open(manifest_path).read()}")
        logging.debug(f"script.js:\n{open(script_path).read()}")
        return temp_dir
        
    except Exception as e:
        _cleanup_extension(temp_dir)
        raise Exception(f"Create extension failed: {e}")

def _cleanup_extension(path: str):
    try:
        if os.path.exists(path):
            shutil.rmtree(path)
    except Exception as e:
        print(f"Cleanup extension file failed: {e}")

def get_patched_browser(options: ChromiumOptions = None,headless = True) -> ChromiumPage:
    """
    Create a browser instance with Turnstile bypass functionality.
    Args:
        options: A ChromiumOptions object. If None, a default configuration will be created.
    Returns:
        Chromium: The configured browser instance.
    """
    platform_id = "Windows NT 10.0; Win64; x64"
    if sys.platform == "linux" or sys.platform == "linux2":
        platform_id = "X11; Linux x86_64"
    elif sys.platform == "darwin":
        platform_id = "Macintosh; Intel Mac OS X 10_15_7"
    user_agent =f"Mozilla/5.0 ({platform_id}) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/136.0.7103.113 Safari/537.36"

    if options is None:
        options = ChromiumOptions().auto_port()

    if headless is True:
        options.headless(True)

    options.set_user_agent(user_agent)
    options.set_argument('--disable-dev-shm-usage')
    options.set_argument('--disable-gpu')
    options.set_argument("--no-sandbox")
    options.set_argument('--lang=zh-TW')
    options.set_argument('--intl.accept_languages=zh-TW,zh')
    options.set_argument('--window-size=1280,800')
    options.set_argument('--start-maximized')
    options.set_argument('--disable-blink-features=AutomationControlled')

    if "--blink-settings=imagesEnabled=false" in options._arguments:
        raise RuntimeError("To bypass Turnstile, imagesEnabled must be True")
    if "--incognito" in options._arguments:
        raise RuntimeError("Cannot bypass Turnstile in incognito mode. Please run in normal browser mode.")
    
    try:
        extension_path = _create_extension()
        options.add_extension(extension_path)
        page = ChromiumPage(options)
        page.run_js("""
            Object.defineProperty(navigator, 'languages', { get: () => ['zh-TW', 'zh'] });
            """)
        logging.debug("[Debug] navigator.webdriver:", page.run_js("return navigator.webdriver"))
        logging.debug("[Debug] navigator.languages:", page.run_js("return navigator.languages"))
        logging.debug("[Debug] navigator.plugins:", page.run_js("return navigator.plugins.length"))
        logging.info(f"[Debug] page UA: {page.run_js('return navigator.userAgent')}")

        shutil.rmtree(extension_path)
        return page
    
    except Exception as e:
        if 'extension_path' in locals() and os.path.exists(extension_path):
            shutil.rmtree(extension_path)
        raise e

def wait_for_turnstile(page, max_retries=10, wait_time=5):
    """
    Wait for Turnstile verification to complete and return the token.
    :param driver: Selenium WebDriver
    :param max_retries: Maximum number of retries
    :param wait_time: Wait time between retries (in seconds)
    :return: Turnstile token or None
    """
    logging.debug("Waiting for Turnstile verification to complete...")
    retries = 0
    while retries < max_retries:
        try:
            logging.info(f"Checking for Turnstile response input field, attempt {retries + 1}/{max_retries}...")
            has_input = page.run_js(
                "return !!document.querySelector('input[name=\"cf-turnstile-response\"]')"
            )
            print(f"[Debug] cf-turnstile-response present: {has_input}")

            # Attempt to retrieve the Turnstile token
            token = page.ele('xpath://input[@name="cf-turnstile-response"]').value
            if token and token.strip():  # If the token exists and is not empty
                logging.debug(f"Retrieved Turnstile token: {token}")
                return token
            else:
                retries += 1
                logging.debug(f"Turnstile token is empty, retrying ({retries}/{max_retries})...")
                time.sleep(wait_time)
                page.get_screenshot(".", f"{retries}")
                logging.info(f"screenshot saved for retry {retries}")
        except Exception as e:
            retries += 1
            logging.debug(f"Error while retrieving Turnstile token, retrying ({retries}/{max_retries}): {e}")
            time.sleep(wait_time)

    logging.error("Reached maximum retries, unable to complete Turnstile verification.")
    return None

def crawl_stock_data(stock, token, output_path):
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
        response = requests.post(url, data=payload, headers=headers, verify=False)
        response.raise_for_status()
        if response.ok:
            if 'tables' in response.json():
                table = response.json()['tables'][1]
                pdf = pd.DataFrame(table['data'], columns=table['fields'])
                pdf['日期'] = datetime.now().strftime('%Y/%m/%d')
                pdf['券商'] = pdf['券商'].str.split().str[0]
                pdf.drop(columns=['序號'], inplace=True)
                pdf.to_csv(output_path, encoding='utf8', index=False)
                logging.info(f"Successfully crawled and saved data for stock {stock}")
            return True
    except Exception as e:
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
    symbols = sys.argv[1][1:-1].replace("'", '').replace(' ' ,'').split(',')  # list of symbols
    data_dt = sys.argv[2] if len(sys.argv) > 2 else datetime.now().strftime('%Y%m%d')
    logging.info(f"Received symbols: {symbols}")
    logging.info(f"Data date: {data_dt}")

    output_dir = "./output/"
    os.makedirs(output_dir, exist_ok=True)

    fs_client = firestore.Client()
    # Process each symbol
    for symbol in symbols:
        logging.info(f"Starting to crawl stock {symbol}")

        # Check if the symbol document exists in Firestore
        try:
            collection_name = f"crawl_status_{data_dt}"
            doc_ref = fs_client.collection(collection_name).document(symbol)
            doc = doc_ref.get()
            if not doc.exists:
                logging.debug(f"Symbol {symbol} not found in Firestore, skipping.")
                continue
        except Exception as e:
            logging.error(f"Failed to access Firestore for symbol {symbol}: {e}")
            continue
        output_path = os.path.join(output_dir, f"{symbol}.csv")
        success = False
        retries = 0 
        while retries < 3 and not success:
            retries += 1
            logging.info(f"start get patched browser")
            page = get_patched_browser(headless=False)
            logging.info(f"finish get patched browser")

            try:
                # Open target page
                logging.info(f"start get tpex page")
                page.get("https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html")
                logging.info(f"finish get tpex page")

                # Wait for Turnstile verification
                token = wait_for_turnstile(page)
                if not token:
                    logging.error(f"Unable to retrieve Turnstile token, skipping stock {symbol}")
                    continue

                # Crawl data
                success = crawl_stock_data(symbol, token, output_path) 
            finally:
                page.quit()
                logging.info(f"Driver for stock {symbol} closed after {retries} retries")

        if success:
            if os.path.exists(output_path):
                # Upload to Google Cloud Storage
                bucket_name = 'stock-crawler-bucket-20250908'
                gcs_blob_name = f'bs_report/{data_dt}/{symbol}.csv'
                upload_to_gcs(bucket_name, output_path, gcs_blob_name)

            # Delete the symbol document from Firestore
            try:
                collection_name = f"crawl_status_{data_dt}"
                
                doc_ref = fs_client.collection(collection_name).document(symbol)
                doc_ref.delete()
                logging.info(f"Deleted Firestore document for symbol: {symbol}")
            except Exception as e:
                logging.error(f"Failed to delete Firestore document for symbol {symbol}: {e}")
        else:
            logging.error(f"Failed to crawl data for stock {symbol} after 3 retries")

if __name__ == "__main__":
    main()