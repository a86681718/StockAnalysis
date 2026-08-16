#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path
import shutil
import tempfile


ROOT = Path(__file__).resolve().parents[1]
SERVICE_NAMES = (
    "prepare-twse-list",
    "prepare-tpex-list",
    "trigger-twse-job",
    "trigger-tpex-job",
)


def stage_service_source(service_name: str, destination: Path) -> Path:
    if service_name not in SERVICE_NAMES:
        raise ValueError(f"Unknown Cloud Run service: {service_name}")
    if destination.exists() and any(destination.iterdir()):
        raise ValueError(f"Staging destination must be empty: {destination}")

    service_source = ROOT / "deployment" / service_name
    package_source = ROOT / "src/stockanalysis"
    destination.mkdir(parents=True, exist_ok=True)

    for source in service_source.iterdir():
        target = destination / source.name
        if source.is_dir():
            shutil.copytree(source, target)
        else:
            shutil.copy2(source, target)

    package_root = destination / "stockanalysis"
    package_root.mkdir()
    (package_root / "__init__.py").write_text(
        '"""Minimal StockAnalysis package staged for Cloud Run source deploy."""\n',
        encoding="utf-8",
    )
    shutil.copytree(package_source / "contracts", package_root / "contracts")
    shutil.copytree(package_source / "workflows", package_root / "workflows")
    return destination


def main() -> int:
    parser = argparse.ArgumentParser(description="Stage one Cloud Run service with shared contracts.")
    parser.add_argument("service", choices=SERVICE_NAMES)
    args = parser.parse_args()

    destination = Path(tempfile.mkdtemp(prefix=f"stockanalysis-{args.service}-"))
    print(stage_service_source(args.service, destination))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
