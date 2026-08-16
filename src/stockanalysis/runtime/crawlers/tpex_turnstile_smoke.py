"""Minimal proxy and Turnstile smoke test for the TPEX broker page."""

import logging
from urllib.parse import urlparse

from stockanalysis.runtime.crawlers.tpex_local_runner import BrowserManager, load_proxies


TARGET_URL = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"


def proxy_label(proxy: str) -> str:
    if not proxy:
        return "direct"
    parsed = urlparse(proxy)
    return f"{parsed.hostname}:{parsed.port or 80}"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    proxy = load_proxies()[0]
    logging.info("Testing TPEX Turnstile through proxy %s", proxy_label(proxy))

    browser = BrowserManager(proxy=proxy)
    try:
        token = browser.get_token(TARGET_URL)
    finally:
        browser.close()

    if not token:
        logging.error("Proxy smoke test did not obtain a Turnstile token")
        return 1

    logging.info("Proxy smoke test obtained a Turnstile token (length=%s)", len(token))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
