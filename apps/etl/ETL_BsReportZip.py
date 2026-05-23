import sys
from pathlib import Path

_SRC_ROOT = Path(__file__).resolve().parents[2] / "src"
if str(_SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(_SRC_ROOT))
import zipfile
from concurrent.futures import ThreadPoolExecutor
import threading
import traceback

import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa
import tqdm

from stockanalysis.config import ensure_dir, resolve_data

ZIP_DIR = resolve_data("bs_report")  # 放 ZIP 的資料夾
OUTPUT_DIR = ensure_dir(resolve_data("bs_report", "parquet"))  # 轉換後的輸出位置
CSV_SUBPATH = "twse"                  # ZIP 內的子目錄

file_locks = {}
file_locks_lock = threading.Lock()

def get_lock(stock_id):
    with file_locks_lock:
        if stock_id not in file_locks:
            file_locks[stock_id] = threading.Lock()
        return file_locks[stock_id]

def safe_convert_numeric(series):
    return pd.to_numeric(series.str.replace(",", ""), errors="coerce")

def process_zip(zip_path: Path):
    date = zip_path.stem.split("_")[-1]
    with zipfile.ZipFile(zip_path, "r") as z:
        csv_files = [f for f in z.namelist() if f.startswith(CSV_SUBPATH) and f.endswith(".csv")]
        for csv_name in csv_files:
            stock_id = Path(csv_name).stem
            try:
                with z.open(csv_name) as f:
                    df = pd.read_csv(f, dtype=str).fillna("")
                    df["日期"] = pd.to_datetime(date)
                    for col in ["價格", "買進股數", "賣出股數"]:
                        if col in df.columns:
                            df[col] = safe_convert_numeric(df[col]).astype("float64")
                    write_parquet_incremental(stock_id, df)
            except Exception as e:
                print(f"[WARN] {csv_name} in {zip_path.name} 讀取失敗: {e}")
                traceback.print_exc()
                continue

def write_parquet_incremental(stock_id, df):
    out_file = OUTPUT_DIR / f"{stock_id}.parquet"
    lock = get_lock(stock_id)
    with lock:
        try:
            new_table = pa.Table.from_pandas(df, preserve_index=False)
            if out_file.exists():
                old_table = pq.read_table(out_file)
                
                # 🔹 若舊檔欄位為 int64，統一轉為 float64
                schema = old_table.schema
                cast_types = {}
                for field in schema:
                    if field.name in ["價格", "買進股數", "賣出股數"]:
                        if pa.types.is_integer(field.type):
                            cast_types[field.name] = pa.float64()
                if cast_types:
                    old_table = old_table.cast(pa.schema([
                        pa.field(name, cast_types.get(name, field.type))
                        for name, field in zip(schema.names, schema)
                    ]))

                all_fields = sorted(set(old_table.schema.names) | set(new_table.schema.names))
                old_table = old_table.select([f for f in all_fields if f in old_table.schema.names])
                new_table = new_table.select([f for f in all_fields if f in new_table.schema.names])
                
                combined = pa.concat_tables(
                    [old_table, new_table],
                    promote_options="default"
                )
                pq.write_table(combined, out_file, compression="zstd")
            else:
                pq.write_table(new_table, out_file, compression="zstd")

        except pa.ArrowTypeError as e:
            print(f"[WARN] {stock_id} schema mismatch, retrying as float64: {e}")
            for col in ["價格", "買進股數", "賣出股數"]:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
            table = pa.Table.from_pandas(df, preserve_index=False)
            pq.write_table(table, out_file, compression="zstd")

        except Exception as e:
            print(f"[ERROR] 寫入 {stock_id}.parquet 出錯: {e}")
            traceback.print_exc()

# === 主程式 ===
print(ZIP_DIR)
zip_files = sorted(ZIP_DIR.glob("*.zip"))
print(f"共找到 {len(zip_files)} 個 ZIP，開始並行處理...")

with ThreadPoolExecutor(max_workers=6) as executor:
    list(tqdm.tqdm(executor.map(process_zip, zip_files), total=len(zip_files)))

print("✅ 全部完成！")
