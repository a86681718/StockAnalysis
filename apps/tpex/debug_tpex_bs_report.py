import json
import logging
import os
import sys
import time
import traceback
from datetime import datetime
from pathlib import Path

import requests
from google.cloud import storage

from stockanalysis.runtime.crawlers.tpex_debug_browser import get_patched_browser


BROKER_PAGE_URL = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"
BROKER_API_URL = "https://www.tpex.org.tw/www/zh-tw/afterTrading/brokerBS"
IP_CHECK_URLS = (
    "https://api.ipify.org?format=json",
    "https://ifconfig.me/ip",
)


def parse_symbols(raw: str) -> list[str]:
    return raw[1:-1].replace("'", "").replace(" ", "").split(",")


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def save_screenshot(page, output_dir: Path, name: str) -> None:
    try:
        page.get_screenshot(str(output_dir), name)
        logging.info("screenshot saved: %s/%s.jpg", output_dir, name)
    except Exception:
        logging.exception("failed to save screenshot: %s", name)


def request_public_ip() -> dict[str, str]:
    results = {}
    for url in IP_CHECK_URLS:
        try:
            response = requests.get(url, timeout=15)
            results[url] = response.text.strip()
            logging.info("requests public IP via %s: %s", url, results[url])
        except Exception as exc:
            results[url] = f"ERROR: {exc}"
            logging.warning("requests public IP failed via %s: %s", url, exc)
    return results


def browser_public_ip(page, output_dir: Path) -> dict[str, str]:
    results = {}
    for index, url in enumerate(IP_CHECK_URLS, start=1):
        try:
            page.get(url)
            save_screenshot(page, output_dir, f"browser-ip-{index}")
            body_text = page.run_js("return document.body ? document.body.innerText : ''") or ""
            results[url] = body_text.strip()
            logging.info("browser public IP via %s: %s", url, results[url])
        except Exception as exc:
            results[url] = f"ERROR: {exc}"
            logging.warning("browser public IP failed via %s: %s", url, exc)
    return results


def collect_browser_fingerprint(page) -> dict[str, object]:
    script = """
    return {
      userAgent: navigator.userAgent,
      webdriver: navigator.webdriver,
      languages: navigator.languages,
      platform: navigator.platform,
      hardwareConcurrency: navigator.hardwareConcurrency,
      deviceMemory: navigator.deviceMemory || null,
      timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
      screen: {
        width: screen.width,
        height: screen.height,
        colorDepth: screen.colorDepth
      },
      window: {
        innerWidth: window.innerWidth,
        innerHeight: window.innerHeight
      }
    };
    """
    return page.run_js(script)


def wait_for_turnstile_debug(page, output_dir: Path, max_retries: int, wait_time: int) -> str | None:
    for attempt in range(1, max_retries + 1):
        try:
            details = page.run_js(
                """
                const input = document.querySelector('input[name="cf-turnstile-response"]');
                const iframe = document.querySelector('iframe[src*="challenges.cloudflare.com"]');
                const widget = document.querySelector('#myWidget');
                return {
                  hasInput: !!input,
                  tokenLength: input && input.value ? input.value.length : 0,
                  hasIframe: !!iframe,
                  iframeSrc: iframe ? iframe.src : null,
                  widgetText: widget ? widget.innerText : null,
                  documentTitle: document.title,
                  location: location.href,
                  bodyTextStart: document.body ? document.body.innerText.slice(0, 500) : null
                };
                """
            )
            logging.info("turnstile attempt %s/%s: %s", attempt, max_retries, json.dumps(details, ensure_ascii=False))
            write_text(
                output_dir / f"turnstile-attempt-{attempt:02d}.json",
                json.dumps(details, ensure_ascii=False, indent=2),
            )
            save_screenshot(page, output_dir, f"turnstile-attempt-{attempt:02d}")
            token = page.run_js(
                """
                const input = document.querySelector('input[name="cf-turnstile-response"]');
                return input ? input.value : '';
                """
            )
            if token and token.strip():
                logging.info("turnstile token acquired, length=%s", len(token))
                return token
        except Exception:
            logging.exception("turnstile debug attempt failed: %s/%s", attempt, max_retries)
        time.sleep(wait_time)
    logging.error("turnstile token was not acquired")
    return None


def post_broker_api(symbol: str, token: str | None, output_dir: Path) -> bool:
    headers = {
        "Content-Type": "application/x-www-form-urlencoded; charset=UTF-8",
        "Origin": "https://www.tpex.org.tw",
        "Referer": BROKER_PAGE_URL,
        "User-Agent": "Mozilla/5.0",
    }
    payload = {
        "cf-turnstile-response": token or "",
        "code": symbol,
    }
    try:
        response = requests.post(BROKER_API_URL, data=payload, headers=headers, timeout=30, verify=False)
        body = response.text
        logging.info("brokerBS POST symbol=%s status=%s body_start=%s", symbol, response.status_code, body[:300])
        write_text(output_dir / f"broker-post-{symbol}.json", body)
        response.raise_for_status()
        data = response.json()
        return "tables" in data
    except Exception:
        logging.exception("brokerBS POST failed for symbol=%s", symbol)
        return False


def upload_debug_artifacts(output_dir: Path) -> None:
    if os.getenv("TPEX_DEBUG_UPLOAD_GCS", "1") == "0":
        logging.info("debug artifact upload disabled")
        return
    bucket_name = os.getenv("STOCK_CRAWLER_BUCKET")
    if not bucket_name:
        logging.info("STOCK_CRAWLER_BUCKET not set; debug artifacts remain local at %s", output_dir)
        return

    prefix = os.getenv("TPEX_DEBUG_GCS_PREFIX", "debug/tpex-bs-report").strip("/")
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    uploaded = 0
    for path in output_dir.rglob("*"):
        if not path.is_file():
            continue
        blob_name = f"{prefix}/{output_dir.name}/{path.relative_to(output_dir)}"
        bucket.blob(blob_name).upload_from_filename(str(path))
        uploaded += 1
        logging.info("uploaded debug artifact: gs://%s/%s", bucket_name, blob_name)
    logging.info("uploaded %s debug artifacts from %s", uploaded, output_dir)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    if len(sys.argv) < 3:
        logging.error("usage: debug_tpex_bs_report.py \"['1580']\" YYYYMMDD")
        return 2

    symbols = parse_symbols(sys.argv[1])
    data_dt = sys.argv[2]
    output_dir = Path(os.getenv("TPEX_DEBUG_OUTPUT_DIR", "/tmp/tpex-debug")) / data_dt / datetime.utcnow().strftime("%Y%m%dT%H%M%SZ")
    output_dir.mkdir(parents=True, exist_ok=True)
    max_retries = int(os.getenv("TPEX_TURNSTILE_RETRIES", "10"))
    wait_time = int(os.getenv("TPEX_TURNSTILE_WAIT_SECONDS", "5"))

    logging.info("debug output dir: %s", output_dir)
    logging.info("received symbols: %s", symbols)
    logging.info("data date: %s", data_dt)
    logging.info("container env DISPLAY=%s", os.getenv("DISPLAY"))
    write_text(output_dir / "argv.json", json.dumps({"argv": sys.argv, "symbols": symbols, "date": data_dt}, ensure_ascii=False, indent=2))

    request_ips = request_public_ip()
    write_text(output_dir / "requests-public-ip.json", json.dumps(request_ips, ensure_ascii=False, indent=2))

    page = None
    exit_code = 1
    try:
        logging.info("starting patched browser")
        page = get_patched_browser(headless=False)
        logging.info("patched browser started")
        save_screenshot(page, output_dir, "browser-started")

        browser_ips = browser_public_ip(page, output_dir)
        write_text(output_dir / "browser-public-ip.json", json.dumps(browser_ips, ensure_ascii=False, indent=2))

        logging.info("loading TPEX broker page: %s", BROKER_PAGE_URL)
        page.get(BROKER_PAGE_URL)
        logging.info("loaded TPEX broker page: %s", page.url)
        save_screenshot(page, output_dir, "tpex-page-loaded")
        fingerprint = collect_browser_fingerprint(page)
        logging.info("browser fingerprint: %s", json.dumps(fingerprint, ensure_ascii=False))
        write_text(output_dir / "browser-fingerprint.json", json.dumps(fingerprint, ensure_ascii=False, indent=2))

        token = wait_for_turnstile_debug(page, output_dir, max_retries=max_retries, wait_time=wait_time)
        write_text(output_dir / "turnstile-token-summary.json", json.dumps({"present": bool(token), "length": len(token) if token else 0}, indent=2))

        ok_count = 0
        for symbol in symbols:
            if post_broker_api(symbol, token, output_dir):
                ok_count += 1
        logging.info("brokerBS POST summary: ok=%s total=%s", ok_count, len(symbols))
        exit_code = 0 if token and ok_count == len(symbols) else 1
    except Exception as exc:
        logging.exception("debug run failed")
        write_text(
            output_dir / "unhandled-exception.json",
            json.dumps(
                {
                    "type": type(exc).__name__,
                    "message": str(exc),
                    "traceback": traceback.format_exc(),
                },
                ensure_ascii=False,
                indent=2,
            ),
        )
    finally:
        if page is not None:
            page.quit()
            logging.info("browser closed")
        try:
            upload_debug_artifacts(output_dir)
        except Exception:
            logging.exception("failed to upload debug artifacts")
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
