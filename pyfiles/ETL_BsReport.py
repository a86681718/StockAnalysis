import pandas as pd
import numpy as np
from pathlib import Path
import pyarrow.parquet as pq
import pyarrow as pa
from concurrent.futures import ThreadPoolExecutor
import tqdm
import traceback
import threading

INPUT_DIR = Path("/Users/fang/Desktop/bs_report/bs_report/twse") # 放每日資料夾的根目錄
OUTPUT_DIR = Path("/Users/fang/Desktop/bs_report/parquet_twse") # 轉換後的輸出位置
OUTPUT_DIR.mkdir(exist_ok=True)

file_locks = {}
file_locks_lock = threading.Lock()

def get_lock(stock_id):
    with file_locks_lock:
        if stock_id not in file_locks:
            file_locks[stock_id] = threading.Lock()
        return file_locks[stock_id]

def safe_convert_numeric(series):
    return pd.to_numeric(series.str.replace(",", ""), errors="coerce")

def process_daily_folder(day_path: Path):
    date_str = day_path.name
    try:
        date = pd.to_datetime(date_str, format="%Y%m%d", errors="raise")
    except ValueError:
        print(f"[WARN] {day_path} 不是有效的日期資料夾，已跳過")
        return

    csv_files = sorted(day_path.glob("*.csv"))
    if not csv_files:
        print(f"[WARN] {day_path} 沒有找到 CSV 檔案")
        return

    for csv_path in csv_files:
        stock_id = csv_path.stem
        try:
            df = pd.read_csv(csv_path, dtype=str).fillna("")
            df["日期"] = pd.to_datetime(date)
            for col in ["價格", "買進股數", "賣出股數"]:
                if col in df.columns:
                    df[col] = safe_convert_numeric(df[col]).astype("float64")
            write_parquet_incremental(stock_id, df)
        except Exception as e:
            print(f"[WARN] {csv_path} 讀取失敗: {e}")
            traceback.print_exc()
            continue

def write_parquet_incremental(stock_id, df):
    out_file = OUTPUT_DIR / f"{stock_id}.parquet"
    lock = get_lock(stock_id)
    with lock:
        try:
            df = df.copy()
            df["日期"] = pd.to_datetime(df["日期"])  # ensure datetime

            if out_file.exists():
                try:
                    existing_df = pd.read_parquet(out_file)
                except Exception:
                    existing_df = pq.read_table(out_file).to_pandas()

                existing_df["日期"] = pd.to_datetime(existing_df["日期"], errors="coerce")

                # 對齊欄位
                for col in existing_df.columns:
                    if col not in df.columns:
                        df[col] = np.nan
                for col in df.columns:
                    if col not in existing_df.columns:
                        existing_df[col] = np.nan

                df = df[existing_df.columns]

                key_cols = [c for c in ["日期", "券商", "價格", "買進股數", "賣出股數"] if c in df.columns]

                combined = pd.concat([existing_df, df], ignore_index=True)
                if key_cols:
                    combined.sort_values(key_cols, inplace=True)
                    combined.drop_duplicates(subset=key_cols, inplace=True, keep="last")
                combined.sort_values("日期", inplace=True)
            else:
                combined = df

            combined.to_parquet(out_file, index=False, compression="zstd")

        except Exception as e:
            print(f"[ERROR] 寫入 {stock_id}.parquet 出錯: {e}")
            traceback.print_exc()

# === 主程式 ===
day_folders = sorted([p for p in INPUT_DIR.iterdir() if p.is_dir()])
print(f"共找到 {len(day_folders)} 個日期資料夾，開始並行處理...")

with ThreadPoolExecutor(max_workers=6) as executor:
    list(tqdm.tqdm(executor.map(process_daily_folder, day_folders), total=len(day_folders)))

print("✅ 全部完成！")
