#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path


for candidate in (
    Path(__file__).resolve().parents[1] / "src",
    Path(__file__).resolve().parents[2] / "src",
):
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        break


if __name__ == "__main__":
    runpy.run_module("stockanalysis.runtime.crawlers.twse_bs_report", run_name="__main__")
