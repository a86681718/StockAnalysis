from __future__ import annotations

import argparse
import sys
from pathlib import Path


_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from stockanalysis.config import ensure_dir, resolve_data
from stockanalysis.pipelines.ohlc import (
    OHLC_PATTERN,
    build_ohlc,
    load_ohlc_csv,
    to_number,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Build data/_derived/ohlc.parquet from raw TWSE/TPEX OHLC CSV files."
        )
    )
    parser.add_argument("--input-dir", type=Path, default=resolve_data("ohlc"))
    parser.add_argument(
        "--output",
        type=Path,
        default=resolve_data("_derived", "ohlc.parquet"),
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    ohlc = build_ohlc(args.input_dir)
    ensure_dir(args.output.parent)
    ohlc.to_parquet(args.output, index=False)

    print(f"wrote {args.output}")
    print(f"rows {len(ohlc)}")
    print(f"symbols {ohlc['symbol'].nunique()}")
    print(f"date_min {ohlc['date'].min()}")
    print(f"date_max {ohlc['date'].max()}")


if __name__ == "__main__":
    main()
