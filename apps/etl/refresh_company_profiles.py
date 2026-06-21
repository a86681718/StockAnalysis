#!/usr/bin/env python3
"""Refresh official TWSE and TPEx company profile snapshots."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[2]
SOURCES = {
    "company_profiles_twse.json": "https://openapi.twse.com.tw/v1/opendata/t187ap03_L",
    "company_profiles_tpex.json": "https://www.tpex.org.tw/openapi/v1/mopsfin_t187ap03_O",
}


def download_json(url: str) -> bytes:
    request = Request(url, headers={"User-Agent": "StockAnalysis/1.0"})
    with urlopen(request, timeout=60) as response:
        content = response.read()
    parsed = json.loads(content.decode("utf-8-sig"))
    if not isinstance(parsed, list) or not parsed:
        raise ValueError(f"Expected a non-empty JSON list from {url}")
    return json.dumps(parsed, ensure_ascii=False, separators=(",", ":")).encode("utf-8") + b"\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Refresh official company-address snapshots.")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data" / "reference")
    args = parser.parse_args()
    output_dir = args.output_dir.expanduser().resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    for filename, url in SOURCES.items():
        output = output_dir / filename
        temporary = output.with_suffix(output.suffix + ".tmp")
        content = download_json(url)
        temporary.write_bytes(content)
        temporary.replace(output)
        print(f"wrote {output} ({len(content)} bytes)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
