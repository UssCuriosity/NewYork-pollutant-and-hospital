"""
fetch_real_epa_bulk.py
======================
从 EPA AQS 公开预构建批量文件（无需 API Key/注册）下载真实污染数据。

预构建文件地址（全国数据 ZIP，无需认证）：
  https://aqs.epa.gov/aqsweb/airdata/daily_{param_code}_{year}.zip

本脚本：
  1. 下载 ZIP（流式，避免内存溢出）
  2. 直接解压到内存，读取 CSV
  3. 筛选 NYC 五县（State Code=36，County Code in 005/047/061/081/085）
  4. 保存为 data/raw/epa_aqs_{param}_{county}_{year}.csv
     及  data/raw/epa_aqs_{param}_{year}.csv（五县合并）
"""

from __future__ import annotations

import io
import logging
import sys
import time
import zipfile
from pathlib import Path

import pandas as pd
import requests
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH  = PROJECT_ROOT / "config.yaml"
RAW_DIR      = PROJECT_ROOT / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "outputs" / "pipeline.log",
                            mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)

BULK_BASE = "https://aqs.epa.gov/aqsweb/airdata"

# EPA AQS 批量文件中的列名
AQS_COL_STATE   = "State Code"
AQS_COL_COUNTY  = "County Code"
AQS_COL_DATE    = "Date Local"
AQS_COL_MEAN    = "Arithmetic Mean"
AQS_COL_OBS     = "Observation Count"
AQS_COL_UNITS   = "Units of Measure"
AQS_COL_SITE    = "Site Num"

PARAM_CODE_TO_NAME = {
    "88101": "pm25",
    "42602": "no2",
    "42401": "so2",
}

COUNTY_CODE_TO_NAME = {
    "005": "bronx",
    "047": "kings",
    "061": "new_york",
    "081": "queens",
    "085": "richmond",
}


def _download_zip_to_memory(url: str, max_retries: int = 3) -> bytes:
    """下载 ZIP 文件到内存，带重试。"""
    for attempt in range(1, max_retries + 1):
        try:
            log.info("  Downloading %s (attempt %d/%d) …", url.split("/")[-1], attempt, max_retries)
            resp = requests.get(url, stream=True, timeout=300)
            resp.raise_for_status()
            data = b""
            total = 0
            for chunk in resp.iter_content(chunk_size=1024 * 1024):  # 1 MB chunks
                data += chunk
                total += len(chunk)
                if total % (10 * 1024 * 1024) == 0:
                    log.info("    … %.0f MB downloaded", total / 1024 / 1024)
            log.info("  Download complete: %.1f MB", total / 1024 / 1024)
            return data
        except requests.RequestException as exc:
            log.warning("  Download failed (attempt %d): %s", attempt, exc)
            if attempt < max_retries:
                time.sleep(10 * attempt)
    raise RuntimeError(f"Failed to download {url} after {max_retries} attempts")


def fetch_bulk_param_year(
    param_name: str,
    param_code: str,
    year: int,
    nyc_county_codes: list[str],
    state_code: str = "36",
) -> dict[str, pd.DataFrame]:
    """
    下载单参数单年度全国 ZIP，筛选 NYC 五县，
    返回 {county_code: DataFrame} 字典。
    """
    url = f"{BULK_BASE}/daily_{param_code}_{year}.zip"
    zip_bytes = _download_zip_to_memory(url)

    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_name = f"daily_{param_code}_{year}.csv"
        if csv_name not in zf.namelist():
            # 尝试找任何 CSV
            csvs = [n for n in zf.namelist() if n.endswith(".csv")]
            if not csvs:
                raise ValueError(f"No CSV found in ZIP for {param_name} {year}")
            csv_name = csvs[0]

        log.info("  Reading %s from ZIP …", csv_name)
        with zf.open(csv_name) as f:
            df_national = pd.read_csv(f, low_memory=False, dtype=str)

    log.info("  National records: %d", len(df_national))

    # 标准化列名（处理空格差异）
    df_national.columns = [c.strip() for c in df_national.columns]

    # 筛选纽约州五县
    state_col  = next((c for c in df_national.columns if "State" in c and "Code" in c), None)
    county_col = next((c for c in df_national.columns if "County" in c and "Code" in c), None)

    if state_col is None or county_col is None:
        log.error("  Cannot find State/County Code columns. Available: %s", list(df_national.columns)[:15])
        return {}

    # 县代码统一为 3 位字符串
    df_national[county_col] = df_national[county_col].str.zfill(3)
    df_national[state_col]  = df_national[state_col].str.zfill(2)

    df_nyc = df_national[
        (df_national[state_col] == state_code) &
        (df_national[county_col].isin(nyc_county_codes))
    ].copy()

    log.info("  NYC records: %d rows across %s counties",
             len(df_nyc), df_nyc[county_col].unique().tolist())

    # 按县拆分
    result: dict[str, pd.DataFrame] = {}
    for county_code in nyc_county_codes:
        df_c = df_nyc[df_nyc[county_col] == county_code].copy()
        if df_c.empty:
            log.warning("  No records for county %s (param=%s, year=%d)", county_code, param_name, year)
            continue

        # 字段映射到标准格式
        rename: dict[str, str] = {}
        date_src  = next((c for c in df_c.columns if c == AQS_COL_DATE or "Date Local" in c), None)
        mean_src  = next((c for c in df_c.columns if "Arithmetic Mean" in c), None)
        obs_src   = next((c for c in df_c.columns if "Observation Count" in c), None)
        units_src = next((c for c in df_c.columns if "Units" in c), None)
        site_src  = next((c for c in df_c.columns if "Site Num" in c), None)

        if date_src:  rename[date_src]  = "date"
        if mean_src:  rename[mean_src]  = "arithmetic_mean"
        if obs_src:   rename[obs_src]   = "observation_count"
        if units_src: rename[units_src] = "units_of_measure"
        if site_src:  rename[site_src]  = "site_num"

        df_c = df_c.rename(columns=rename)
        df_c["date"]              = pd.to_datetime(df_c["date"], errors="coerce").dt.strftime("%Y-%m-%d")
        df_c["arithmetic_mean"]   = pd.to_numeric(df_c.get("arithmetic_mean",  pd.Series()), errors="coerce")
        df_c["observation_count"] = pd.to_numeric(df_c.get("observation_count", pd.Series(1, index=df_c.index)), errors="coerce").fillna(1)
        df_c["county_fips"]  = f"{state_code}{county_code}"
        df_c["county_name"]  = COUNTY_CODE_TO_NAME.get(county_code, county_code)
        df_c["param"]        = param_name

        keep = ["date", "county_fips", "county_name", "param",
                "arithmetic_mean", "observation_count"]
        if "units_of_measure" in df_c.columns:
            keep.append("units_of_measure")
        if "site_num" in df_c.columns:
            keep.append("site_num")

        df_c = df_c[[c for c in keep if c in df_c.columns]].dropna(subset=["date", "arithmetic_mean"])
        result[county_code] = df_c

    return result


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "outputs").mkdir(parents=True, exist_ok=True)

    years        = cfg["study"]["years"]
    params_map   = cfg["epa_aqs"]["parameters"]   # pm25→88101 …
    counties     = cfg["epa_aqs"]["county_fips"]  # name→code dict
    state        = cfg["epa_aqs"]["state_fips"]
    county_codes = list(counties.values())

    for year in years:
        for param_name, param_code in params_map.items():
            merged_file = RAW_DIR / f"epa_aqs_{param_name}_{year}.csv"
            if merged_file.exists():
                log.info("[CACHE] %s already exists, skipping.", merged_file.name)
                continue

            log.info("=== Fetching EPA bulk: %s year=%d ===", param_name.upper(), year)
            try:
                county_dfs = fetch_bulk_param_year(
                    param_name=param_name,
                    param_code=param_code,
                    year=year,
                    nyc_county_codes=county_codes,
                    state_code=state,
                )
            except Exception as exc:
                log.error("  FAILED to fetch %s %d: %s", param_name, year, exc)
                continue

            # 保存单县文件
            all_frames = []
            for county_code, df_c in county_dfs.items():
                county_name = COUNTY_CODE_TO_NAME.get(county_code, county_code)
                county_file = RAW_DIR / f"epa_aqs_{param_name}_{county_name}_{year}.csv"
                df_c.to_csv(county_file, index=False)
                log.info("  Saved county file: %s (%d rows)", county_file.name, len(df_c))
                all_frames.append(df_c)

            # 保存五县合并文件
            if all_frames:
                df_merged = pd.concat(all_frames, ignore_index=True)
                df_merged.to_csv(merged_file, index=False)
                log.info("Merged → %s (%d rows)", merged_file.name, len(df_merged))

    log.info("fetch_real_epa_bulk.py completed.")


if __name__ == "__main__":
    run()
