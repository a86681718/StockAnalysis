from __future__ import annotations

import argparse
import json
import shutil
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path


_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))

from stockanalysis.config import ensure_dir, resolve_data

from bs_report_pipeline import BsReportEtl


MARKETS = ("twse", "tpex")


@dataclass
class MarketPaths:
    market: str
    inbox_dir: Path
    archive_dir: Path
    output_dir: Path
    manifest_path: Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync and process bs_report folders for twse/tpex.")
    parser.add_argument("--market", choices=("twse", "tpex", "all"), required=True)
    parser.add_argument("--sync", dest="sync", action="store_true", help="Run gcloud storage rsync before ETL.")
    parser.add_argument("--no-sync", dest="sync", action="store_false", help="Skip GCS sync.")
    parser.set_defaults(sync=False)
    parser.add_argument("--archive", dest="archive", action="store_true", help="Move successful folders to archive.")
    parser.add_argument("--no-archive", dest="archive", action="store_false", help="Keep source folders in inbox.")
    parser.set_defaults(archive=True)
    parser.add_argument("--dry-run", action="store_true", help="Show planned actions without writing changes.")
    parser.add_argument("--gcs-base-uri", default="gs://stock-crawler-bucket-20260302/bs_report")
    parser.add_argument("--max-workers", type=int, default=6)
    parser.add_argument("--limit", type=int, help="Only process the first N pending folders per market.")
    parser.add_argument("--since", help="Only process folders on or after YYYYMMDD.")
    return parser.parse_args()


def build_paths(market: str) -> MarketPaths:
    base = resolve_data("bs_report")
    return MarketPaths(
        market=market,
        inbox_dir=ensure_dir(base / "inbox" / market),
        archive_dir=ensure_dir(base / "archive" / market),
        output_dir=ensure_dir(base / f"parquet_{market}"),
        manifest_path=ensure_dir(base / "manifests") / f"{market}_processed.json",
    )


def load_manifest(path: Path) -> dict[str, dict]:
    if not path.exists():
        return {}
    with path.open("r", encoding="utf-8") as handle:
        data = json.load(handle)
    if isinstance(data, dict):
        return data
    return {}


def save_manifest(path: Path, manifest: dict[str, dict], dry_run: bool) -> None:
    if dry_run:
        return
    with path.open("w", encoding="utf-8") as handle:
        json.dump(manifest, handle, ensure_ascii=False, indent=2, sort_keys=True)


def list_remote_folders(paths: MarketPaths, gcs_base_uri: str) -> list[str]:
    source = f"{gcs_base_uri.rstrip('/')}/{paths.market}/"
    cmd = ["gcloud", "storage", "ls", source]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    folders: list[str] = []
    for line in result.stdout.splitlines():
        folder = line.rstrip("/").split("/")[-1]
        if len(folder) == 8 and folder.isdigit():
            folders.append(folder)
    return sorted(set(folders))


def sync_market(paths: MarketPaths, manifest: dict[str, dict], since: str | None, gcs_base_uri: str, dry_run: bool) -> int:
    remote_folders = list_remote_folders(paths, gcs_base_uri)
    pending_remote_folders = [
        folder_name
        for folder_name in remote_folders
        if (not since or folder_name >= since)
        and manifest.get(folder_name, {}).get("status") != "success"
    ]

    print(f"[SYNC] {paths.market}")
    print(f"  remote folders: {len(remote_folders)}")
    print(f"  folders to copy: {len(pending_remote_folders)}")

    for folder_name in pending_remote_folders:
        source = f"{gcs_base_uri.rstrip('/')}/{paths.market}/{folder_name}"
        cmd = ["gcloud", "storage", "cp", "-r", source, str(paths.inbox_dir)]
        print("[SYNC]", " ".join(cmd))
        if dry_run:
            continue
        subprocess.run(cmd, check=True)

    return len(pending_remote_folders)


def should_process(folder: Path, manifest_entry: dict | None, since: str | None) -> bool:
    if since and folder.name < since:
        return False
    if not folder.is_dir():
        return False
    if manifest_entry and manifest_entry.get("status") == "success":
        return False
    return True


def archive_folder(source: Path, destination_root: Path, dry_run: bool) -> None:
    destination = destination_root / source.name
    if dry_run:
        print(f"[ARCHIVE] {source} -> {destination}")
        return
    if destination.exists():
        shutil.rmtree(destination)
    shutil.move(str(source), str(destination))


def process_market(paths: MarketPaths, args: argparse.Namespace) -> dict[str, int]:
    manifest = load_manifest(paths.manifest_path)
    all_folders = sorted([path for path in paths.inbox_dir.iterdir() if path.is_dir()])
    pending_folders = []
    skipped = 0

    for folder in all_folders:
        entry = manifest.get(folder.name)
        if should_process(folder, entry, args.since):
            pending_folders.append(folder)
        else:
            skipped += 1

    if args.limit is not None:
        pending_folders = pending_folders[: args.limit]

    print(f"[MARKET] {paths.market}")
    print(f"  inbox: {paths.inbox_dir}")
    print(f"  output: {paths.output_dir}")
    print(f"  archive: {paths.archive_dir}")
    print(f"  manifest: {paths.manifest_path}")
    print(f"  total folders: {len(all_folders)}")
    print(f"  pending folders: {len(pending_folders)}")
    print(f"  skipped folders: {skipped}")

    if args.dry_run:
        for folder in pending_folders:
            print(f"[DRY-RUN] would process {folder}")
        return {
            "synced_folders": len(all_folders),
            "processed_folders": 0,
            "processed_csv_files": 0,
            "updated_parquet_files": 0,
            "failed_folders": 0,
            "skipped_folders": skipped,
        }

    etl = BsReportEtl(paths.inbox_dir, paths.output_dir, max_workers=args.max_workers)
    stats, folder_results = etl.run(pending_folders)

    for folder in pending_folders:
        result = folder_results.get(folder.name, {"ok": False, "csv_files": 0})
        now = datetime.now().astimezone().isoformat()
        manifest[folder.name] = {
            "market": paths.market,
            "source_name": folder.name,
            "source_type": "folder",
            "processed_at": now,
            "status": "success" if result["ok"] else "failed",
            "csv_files": result["csv_files"],
        }
        if args.archive and result["ok"]:
            archive_folder(folder, paths.archive_dir, dry_run=False)

    save_manifest(paths.manifest_path, manifest, dry_run=False)
    return {
        "synced_folders": len(all_folders),
        "processed_folders": stats.processed_folders,
        "processed_csv_files": stats.processed_csv_files,
        "updated_parquet_files": stats.updated_parquet_files,
        "failed_folders": stats.failed_folders,
        "skipped_folders": skipped,
    }


def main() -> int:
    args = parse_args()
    markets = MARKETS if args.market == "all" else (args.market,)
    summaries: dict[str, dict[str, int]] = {}

    for market in markets:
        paths = build_paths(market)
        manifest = load_manifest(paths.manifest_path)
        synced_folders = 0
        if args.sync:
            synced_folders = sync_market(paths, manifest, args.since, args.gcs_base_uri, args.dry_run)
        summaries[market] = process_market(paths, args)
        summaries[market]["synced_folders"] = synced_folders

    print("\n[SUMMARY]")
    for market, summary in summaries.items():
        print(f"- {market}")
        print(f"  synced folders: {summary['synced_folders']}")
        print(f"  processed folders: {summary['processed_folders']}")
        print(f"  processed csv files: {summary['processed_csv_files']}")
        print(f"  updated parquet files: {summary['updated_parquet_files']}")
        print(f"  failed folders: {summary['failed_folders']}")
        print(f"  skipped folders: {summary['skipped_folders']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
