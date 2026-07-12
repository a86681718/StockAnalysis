"""Minimal proxy and Turnstile smoke test for the TPEX broker page."""

import logging
import os

from stockanalysis.runtime.crawlers.tpex_bs_report_new import (
    get_turnstile_token_sync,
    load_proxies,
    proxy_label,
)


TARGET_URL = "https://www.tpex.org.tw/zh-tw/mainboard/trading/info/brokerBS.html"


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    proxies = load_proxies()
    proxy = proxies[0]
    os.environ["PROXY"] = proxy
    logging.info("Testing TPEX Turnstile through proxy %s", proxy_label(proxy))

    token = get_turnstile_token_sync(TARGET_URL, "proxy_smoke_test")
    if not token:
        logging.error("Proxy smoke test did not obtain a Turnstile token")
        return 1

    logging.info("Proxy smoke test obtained a Turnstile token (length=%s)", len(token))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
