#!/usr/bin/env python3
from __future__ import annotations

import runpy
import sys
from pathlib import Path


script_path = Path(__file__).resolve()
candidate_roots = [script_path.parent / "src", *(parent / "src" for parent in script_path.parents)]

for candidate in candidate_roots:
    if candidate.exists():
        sys.path.insert(0, str(candidate))
        break


if __name__ == "__main__":
    runpy.run_module("stockanalysis.runtime.crawlers.tpex_bs_report", run_name="__main__")
