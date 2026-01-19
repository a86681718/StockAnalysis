from __future__ import annotations

import os
from pathlib import Path


_DEFAULT_ROOT = Path(__file__).resolve().parents[2]


def project_root() -> Path:
    root = os.getenv("STOCKANALYSIS_ROOT")
    if root:
        return Path(root).expanduser().resolve()
    return _DEFAULT_ROOT


PROJECT_ROOT = project_root()
DATA_DIR = Path(os.getenv("STOCKANALYSIS_DATA_DIR", PROJECT_ROOT / "data")).expanduser().resolve()
OUTPUT_DIR = Path(os.getenv("STOCKANALYSIS_OUTPUT_DIR", PROJECT_ROOT / "outputs")).expanduser().resolve()
ASSETS_DIR = Path(os.getenv("STOCKANALYSIS_ASSETS_DIR", PROJECT_ROOT / "assets")).expanduser().resolve()
CONF_DIR = Path(os.getenv("STOCKANALYSIS_CONF_DIR", PROJECT_ROOT / "conf")).expanduser().resolve()


def resolve_data(*parts: str) -> Path:
    return DATA_DIR.joinpath(*parts)


def resolve_output(*parts: str) -> Path:
    return OUTPUT_DIR.joinpath(*parts)


def resolve_assets(*parts: str) -> Path:
    return ASSETS_DIR.joinpath(*parts)


def ensure_dir(path: Path) -> Path:
    path.mkdir(parents=True, exist_ok=True)
    return path
