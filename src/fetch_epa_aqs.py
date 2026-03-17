"""
fetch_epa_aqs.py
================
从 EPA AQS API 抓取纽约市（NYC）五县的逐日污染物数据：
  - PM2.5  (参数代码 88101)
  - NO2    (参数代码 42602)
  - SO2    (参数代码 42401)

覆盖年份：config.yaml 中 study.years（默认 2022–2024）
输出路径：data/raw/epa_aqs_{param}_{county}_{year}.csv
合并文件：data/raw/epa_aqs_{param}_{year}.csv（五县汇总）

本地回退：若目标文件已存在，跳过 API 请求；
         若 API 凭据为空，要求用户预先将手动下载的 CSV 放到 data/raw/。

手动下载说明（无 API key 时）：
  1. 访问 https://aqs.epa.gov/aqsweb/documents/data_api.html#daily
  2. 选择 Daily Summary Data → Download by County
  3. 参数：State=36 (New York)，County=依次选五县，Parameter Code=88101/42602/42401
  4. 将下载的文件重命名为 epa_aqs_{param}_{county}_{year}.csv 放入 data/raw/
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import yaml

# ── 项目根目录定位 ───────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH  = PROJECT_ROOT / "config.yaml"
RAW_DIR      = PROJECT_ROOT / "data" / "raw"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "outputs" / "pipeline.log", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── EPA AQS API 核心请求 ─────────────────────────────────────

def _aqs_request(
    base_url: str,
    endpoint: str,
    params: dict,
    max_retries: int = 3,
    delay: float = 2.0,
) -> Optional[list[dict]]:
    """向 EPA AQS REST API 发起 GET 请求，含重试逻辑。"""
    url = f"{base_url}/{endpoint}"
    for attempt in range(1, max_retries + 1):
        try:
            resp = requests.get(url, params=params, timeout=60)
            resp.raise_for_status()
            payload = resp.json()
            if payload.get("Header", [{}])[0].get("status") == "Success":
                return payload.get("Data", [])
            else:
                msg = payload.get("Header", [{}])[0].get("error", "Unknown error")
                log.warning("AQS API returned non-success: %s (attempt %d/%d)", msg, attempt, max_retries)
        except requests.RequestException as exc:
            log.warning("Request error on attempt %d/%d: %s", attempt, max_retries, exc)
        if attempt < max_retries:
            time.sleep(delay * attempt)  # 指数退避
    return None


def fetch_daily_by_county(
    email: str,
    key: str,
    base_url: str,
    endpoint: str,
    param_code: str,
    state: str,
    county: str,
    year: int,
    delay: float = 2.0,
) -> Optional[pd.DataFrame]:
    """
    拉取单县单参数单年度的逐日数据。
    返回 DataFrame 或 None（请求失败）。
    """
    bdate = f"{year}0101"
    edate = f"{year}1231"
    params = {
        "email":   email,
        "key":     key,
        "param":   param_code,
        "bdate":   bdate,
        "edate":   edate,
        "state":   state,
        "county":  county,
    }
    log.info("  AQS request → param=%s county=%s%s year=%s", param_code, state, county, year)
    records = _aqs_request(base_url, endpoint, params, delay=delay)
    if records is None:
        return None

    if not records:
        log.warning("  No records returned for param=%s county=%s%s year=%s", param_code, state, county, year)
        return pd.DataFrame()

    df = pd.DataFrame(records)
    return df


# ── 字段标准化 ───────────────────────────────────────────────

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


def standardize_aqs_df(df: pd.DataFrame, param_name: str, county_fips: str) -> pd.DataFrame:
    """
    从 AQS API 返回的宽格式 DataFrame 中提取关键字段，统一命名。

    保留字段：
      date          YYYY-MM-DD
      county_fips   完整 FIPS（州+县，5位）
      county_name   可读名
      param         污染物名称（pm25/no2/so2）
      arithmetic_mean  日均浓度
      units_of_measure
      observation_count  站点观测数（用于后续加权）
      site_num      站点编号（可选）
    """
    if df.empty:
        return df

    # AQS 返回字段名因版本略有不同，做容错映射
    col_map = {
        "date_local":           "date",
        "arithmetic_mean":      "arithmetic_mean",
        "units_of_measure":     "units_of_measure",
        "observation_count":    "observation_count",
        "site_num":             "site_num",
        "county_code":          "county_code",
        "state_code":           "state_code",
    }
    df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})

    # 若 date_local 不存在，尝试备用字段
    if "date" not in df.columns:
        for alt in ["date_gmt", "date"]:
            if alt in df.columns:
                df = df.rename(columns={alt: "date"})
                break

    required = ["date", "arithmetic_mean"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        log.warning("  Missing required columns %s; available: %s", missing, list(df.columns))
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"]).dt.strftime("%Y-%m-%d")
    df["arithmetic_mean"] = pd.to_numeric(df["arithmetic_mean"], errors="coerce")
    df["county_fips"] = f"36{county_fips}"
    df["county_name"] = COUNTY_CODE_TO_NAME.get(county_fips, county_fips)
    df["param"] = param_name
    if "observation_count" not in df.columns:
        df["observation_count"] = 1
    else:
        df["observation_count"] = pd.to_numeric(df["observation_count"], errors="coerce").fillna(1)

    keep = ["date", "county_fips", "county_name", "param",
            "arithmetic_mean", "observation_count"]
    if "units_of_measure" in df.columns:
        keep.append("units_of_measure")
    if "site_num" in df.columns:
        keep.append("site_num")

    return df[[c for c in keep if c in df.columns]].copy()


# ── 本地回退：读取手动下载的 CSV ─────────────────────────────

def load_local_aqs_csv(raw_dir: Path, param_name: str, county_code: str, year: int) -> Optional[pd.DataFrame]:
    """
    尝试读取已下载到 data/raw/ 的 AQS CSV 文件。
    支持两种命名模式：
      1. epa_aqs_{param}_{county}_{year}.csv  （本脚本标准输出名）
      2. ad_viz_plotval_data.csv              （AQS 网站直接下载名，需用户手动重命名）
    """
    county_name = COUNTY_CODE_TO_NAME.get(county_code, county_code)
    candidate = raw_dir / f"epa_aqs_{param_name}_{county_name}_{year}.csv"
    if candidate.exists():
        log.info("  Local CSV found: %s", candidate.name)
        df = pd.read_csv(candidate, low_memory=False)
        return df
    return None


# ── 主流程 ───────────────────────────────────────────────────

def fetch_all_aqs(cfg: dict) -> None:
    """
    遍历 年份 × 污染物参数 × 五县，拉取或读取 AQS 日均数据，
    保存为 data/raw/epa_aqs_{param}_{county}_{year}.csv，
    并合并五县为 data/raw/epa_aqs_{param}_{year}.csv。
    """
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    aqs_cfg   = cfg["epa_aqs"]
    email     = aqs_cfg.get("email", "").strip()
    key       = aqs_cfg.get("key", "").strip()
    base_url  = aqs_cfg["base_url"]
    endpoint  = aqs_cfg["endpoint_daily"]
    delay     = float(aqs_cfg.get("request_delay", 2.0))
    state     = aqs_cfg["state_fips"]
    counties  = aqs_cfg["county_fips"]   # dict: name → code
    params_map = aqs_cfg["parameters"]   # dict: pm25 → "88101" …
    years     = cfg["study"]["years"]

    use_api = bool(email and key)
    if not use_api:
        log.warning(
            "EPA AQS credentials not set in config.yaml. "
            "Will attempt to load local CSVs from data/raw/. "
            "Expected naming: epa_aqs_{param}_{county}_{year}.csv"
        )

    for year in years:
        for param_name, param_code in params_map.items():
            all_counties_frames: list[pd.DataFrame] = []

            for county_name, county_code in counties.items():
                out_file = RAW_DIR / f"epa_aqs_{param_name}_{county_name}_{year}.csv"

                # 1) 已有缓存文件则直接读取
                if out_file.exists():
                    log.info("[CACHE] %s already exists, skipping fetch.", out_file.name)
                    df_raw = pd.read_csv(out_file, low_memory=False)
                    if not df_raw.empty:
                        all_counties_frames.append(df_raw)
                    continue

                df_county: Optional[pd.DataFrame] = None

                # 2) API 拉取
                if use_api:
                    df_raw = fetch_daily_by_county(
                        email=email, key=key,
                        base_url=base_url, endpoint=endpoint,
                        param_code=param_code,
                        state=state, county=county_code,
                        year=year, delay=delay,
                    )
                    if df_raw is not None and not df_raw.empty:
                        df_county = standardize_aqs_df(df_raw, param_name, county_code)
                    time.sleep(delay)

                # 3) API 失败或未配置 → 本地 CSV
                if df_county is None or df_county.empty:
                    df_local = load_local_aqs_csv(RAW_DIR, param_name, county_code, year)
                    if df_local is not None and not df_local.empty:
                        # 如果本地文件已是标准格式，直接使用
                        if "arithmetic_mean" in df_local.columns and "date" in df_local.columns:
                            df_county = df_local
                        else:
                            df_county = standardize_aqs_df(df_local, param_name, county_code)
                    else:
                        log.error(
                            "No data for param=%s county=%s year=%d. "
                            "Place manual download at: %s",
                            param_name, county_name, year, out_file
                        )
                        continue

                if df_county is not None and not df_county.empty:
                    df_county.to_csv(out_file, index=False)
                    log.info("  Saved → %s (%d rows)", out_file.name, len(df_county))
                    all_counties_frames.append(df_county)

            # 合并五县到单文件
            merged_file = RAW_DIR / f"epa_aqs_{param_name}_{year}.csv"
            if merged_file.exists():
                log.info("[CACHE] %s exists, skipping merge.", merged_file.name)
                continue

            if all_counties_frames:
                df_merged = pd.concat(all_counties_frames, ignore_index=True)
                df_merged.to_csv(merged_file, index=False)
                log.info(
                    "Merged → %s (%d rows, %d counties)",
                    merged_file.name, len(df_merged), df_merged["county_fips"].nunique()
                    if "county_fips" in df_merged.columns else "?"
                )
            else:
                log.warning(
                    "No county data collected for param=%s year=%d; "
                    "merged file not created.", param_name, year
                )


def summarize_coverage(cfg: dict) -> None:
    """打印已拉取数据的日期覆盖摘要（用于验收检查）。"""
    params_map = cfg["epa_aqs"]["parameters"]
    years      = cfg["study"]["years"]

    log.info("=== EPA AQS Coverage Summary ===")
    for year in years:
        for param_name in params_map:
            f = RAW_DIR / f"epa_aqs_{param_name}_{year}.csv"
            if f.exists():
                df = pd.read_csv(f, usecols=["date", "arithmetic_mean"], low_memory=False)
                df["date"] = pd.to_datetime(df["date"], errors="coerce")
                n_days = df["date"].dt.date.nunique()
                n_missing = df["arithmetic_mean"].isna().sum()
                log.info(
                    "  %-4s %d: %3d unique dates | %d null values",
                    param_name.upper(), year, n_days, n_missing
                )
            else:
                log.warning("  %-4s %d: FILE NOT FOUND", param_name.upper(), year)


# ── 入口 ─────────────────────────────────────────────────────

if __name__ == "__main__":
    cfg = load_config()
    fetch_all_aqs(cfg)
    summarize_coverage(cfg)
    log.info("fetch_epa_aqs.py completed.")
