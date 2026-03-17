"""
_sparcs_base.py
===============
SPARCS PUF 三类数据集（Inpatient / ED / Outpatient）的公共基础逻辑：
  - Socrata API 分页拉取
  - 本地 CSV 回退
  - 字段标准化与 ICD-10 预过滤
  - 年龄分组映射

本模块不直接运行，被 fetch_sparcs_*.py 调用。
"""

from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from typing import Optional

import pandas as pd
import yaml

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH  = PROJECT_ROOT / "config.yaml"
RAW_DIR      = PROJECT_ROOT / "data" / "raw"

log = logging.getLogger(__name__)


# ── 配置加载 ─────────────────────────────────────────────────

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ── Socrata 分页拉取 ─────────────────────────────────────────

def _fetch_socrata_paged(
    domain: str,
    dataset_id: str,
    app_token: Optional[str],
    where_clause: str,
    select_cols: list[str],
    page_size: int = 50000,
    max_retries: int = 3,
) -> pd.DataFrame:
    """
    使用 sodapy.Socrata 分页拉取数据集。
    若 sodapy 不可用，退回到原始 requests+SoQL。
    """
    try:
        from sodapy import Socrata  # type: ignore
        _fetch_with_sodapy = True
    except ImportError:
        _fetch_with_sodapy = False
        log.warning("sodapy not installed; falling back to direct requests.")

    if _fetch_with_sodapy:
        return _socrata_sodapy(
            domain, dataset_id, app_token, where_clause,
            select_cols, page_size, max_retries
        )
    else:
        return _socrata_requests(
            domain, dataset_id, app_token, where_clause,
            select_cols, page_size, max_retries
        )


def _socrata_sodapy(
    domain: str,
    dataset_id: str,
    app_token: Optional[str],
    where_clause: str,
    select_cols: list[str],
    page_size: int,
    max_retries: int,
) -> pd.DataFrame:
    from sodapy import Socrata  # type: ignore

    client = Socrata(domain, app_token, timeout=120)
    all_records: list[dict] = []
    offset = 0
    select_str = ", ".join(select_cols) if select_cols else "*"

    while True:
        for attempt in range(1, max_retries + 1):
            try:
                batch = client.get(
                    dataset_id,
                    where=where_clause,
                    select=select_str,
                    limit=page_size,
                    offset=offset,
                    order=":id",
                )
                break
            except Exception as exc:
                log.warning("Socrata attempt %d/%d failed: %s", attempt, max_retries, exc)
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                else:
                    log.error("All Socrata retries exhausted for %s offset=%d", dataset_id, offset)
                    batch = []

        if not batch:
            break

        all_records.extend(batch)
        log.info("  Fetched %d records (total so far: %d)", len(batch), len(all_records))

        if len(batch) < page_size:
            break
        offset += page_size
        time.sleep(1.0)

    client.close()
    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


def _socrata_requests(
    domain: str,
    dataset_id: str,
    app_token: Optional[str],
    where_clause: str,
    select_cols: list[str],
    page_size: int,
    max_retries: int,
) -> pd.DataFrame:
    import requests

    base_url = f"https://{domain}/resource/{dataset_id}.json"
    headers  = {"X-App-Token": app_token} if app_token else {}
    select_str = ", ".join(select_cols) if select_cols else "*"
    all_records: list[dict] = []
    offset = 0

    while True:
        params = {
            "$where":  where_clause,
            "$select": select_str,
            "$limit":  page_size,
            "$offset": offset,
            "$order":  ":id",
        }
        for attempt in range(1, max_retries + 1):
            try:
                resp = requests.get(base_url, params=params, headers=headers, timeout=120)
                resp.raise_for_status()
                batch = resp.json()
                break
            except Exception as exc:
                log.warning("Requests attempt %d/%d failed: %s", attempt, max_retries, exc)
                if attempt < max_retries:
                    time.sleep(5 * attempt)
                else:
                    log.error("All retries exhausted for %s offset=%d", dataset_id, offset)
                    batch = []

        if not batch:
            break

        all_records.extend(batch)
        log.info("  Fetched %d records (total so far: %d)", len(batch), len(all_records))

        if len(batch) < page_size:
            break
        offset += page_size
        time.sleep(1.0)

    return pd.DataFrame(all_records) if all_records else pd.DataFrame()


# ── 字段标准化 ───────────────────────────────────────────────

def standardize_sparcs_df(
    df: pd.DataFrame,
    data_type: str,          # "inpatient" | "ed" | "outpatient"
    field_map: dict,         # config.yaml socrata.field_map[data_type]
    age_group_map: dict,     # config.yaml age_group_map
    icd10_prefixes: dict,    # config.yaml icd10_prefixes
    year: int,
) -> pd.DataFrame:
    """
    将原始 SPARCS DataFrame 标准化为以下统一格式：
      date_col        — 日期字符串（YYYY-MM-DD）或 None（月级数据）
      discharge_year  — int
      discharge_month — int 或 NaN
      age_group       — 标准化年龄组
      diagnosis_code  — 清洗后的 ICD-10 代码
      data_type       — "inpatient" / "ed" / "outpatient"
      is_icd10_match  — bool（是否命中白名单）
    """
    if df.empty:
        return pd.DataFrame()

    fmap = field_map.get(data_type, {})

    # 1) 日期/年月字段
    date_field   = fmap.get("discharge_date", "")
    year_field   = fmap.get("discharge_year", "")
    month_field  = fmap.get("discharge_month", "")

    # 检测实际存在的列
    has_date  = date_field  in df.columns and df[date_field].notna().any()
    has_year  = year_field  in df.columns
    has_month = month_field in df.columns and df[month_field].notna().any()

    if has_date:
        df["date_col"] = pd.to_datetime(df[date_field], errors="coerce").dt.strftime("%Y-%m-%d")
        df["discharge_year"]  = pd.to_datetime(df[date_field], errors="coerce").dt.year
        df["discharge_month"] = pd.to_datetime(df[date_field], errors="coerce").dt.month
        df["date_resolution"] = "daily"
    elif has_year and has_month:
        df["discharge_year"]  = pd.to_numeric(df[year_field],  errors="coerce").astype("Int64")
        df["discharge_month"] = pd.to_numeric(df[month_field], errors="coerce").astype("Int64")
        df["date_col"]        = pd.NaT
        df["date_resolution"] = "monthly"
    elif has_year:
        df["discharge_year"]  = pd.to_numeric(df[year_field], errors="coerce").astype("Int64")
        df["discharge_month"] = pd.NA
        df["date_col"]        = pd.NaT
        df["date_resolution"] = "annual"
    else:
        df["discharge_year"]  = year
        df["discharge_month"] = pd.NA
        df["date_col"]        = pd.NaT
        df["date_resolution"] = "annual"

    # 2) 年龄分组
    age_field = fmap.get("age_group", "age_group")
    if age_field in df.columns:
        df["age_group"] = df[age_field].astype(str).str.strip().map(age_group_map)
        # 未命中的尝试部分匹配
        unmapped_mask = df["age_group"].isna()
        if unmapped_mask.any():
            df.loc[unmapped_mask, "age_group"] = (
                df.loc[unmapped_mask, age_field]
                .astype(str)
                .apply(lambda v: _fuzzy_age_map(v, age_group_map))
            )
    else:
        df["age_group"] = "Unknown"

    # 3) ICD-10 主诊断
    diag_field = fmap.get("diagnosis_code", "")
    if diag_field in df.columns:
        df["diagnosis_code"] = (
            df[diag_field].astype(str)
            .str.upper()
            .str.replace(r"\s+", "", regex=True)
        )
    else:
        # 尝试备选列
        for alt in ["icd_diagnosis_code_1", "ccs_diagnosis_code",
                    "apr_drg_code", "principal_diagnosis_code"]:
            if alt in df.columns:
                df["diagnosis_code"] = df[alt].astype(str).str.upper()
                break
        else:
            df["diagnosis_code"] = ""

    # 4) ICD-10 白名单过滤
    all_prefixes: list[str] = []
    for group_prefixes in icd10_prefixes.values():
        all_prefixes.extend(group_prefixes)

    def _matches_any(code: str) -> bool:
        if not code or code in ("NAN", "NONE", ""):
            return False
        return any(code.startswith(p) for p in all_prefixes)

    df["is_icd10_match"] = df["diagnosis_code"].apply(_matches_any)

    # 5) 数据类型标注
    df["data_type"] = data_type

    # 6) 保留并重命名有用列
    keep = [
        "date_col", "discharge_year", "discharge_month",
        "date_resolution", "age_group", "diagnosis_code",
        "is_icd10_match", "data_type",
    ]
    # 保留原始 facility_county 以便验证
    county_field = fmap.get("facility_county", "")
    if county_field in df.columns:
        df["facility_county"] = df[county_field]
        keep.append("facility_county")

    existing_keep = [c for c in keep if c in df.columns]
    return df[existing_keep].copy()


def _fuzzy_age_map(raw_val: str, age_group_map: dict) -> str:
    """对未精确匹配的年龄字段做模糊映射（基于数字提取）。"""
    import re
    nums = re.findall(r"\d+", raw_val)
    if not nums:
        return "Unknown"
    age = int(nums[0])
    if age <= 17:
        return "0-17"
    if age <= 44:
        return "18-44"
    if age <= 64:
        return "45-64"
    if age <= 74:
        return "65-74"
    return "75+"


# ── 主抓取函数（被三个脚本调用）─────────────────────────────

def fetch_sparcs(
    data_type: str,   # "inpatient" | "ed" | "outpatient"
    cfg: Optional[dict] = None,
) -> None:
    """
    拉取指定类型的 SPARCS PUF 数据，
    保存为 data/raw/sparcs_{data_type}_{year}.csv。
    """
    if cfg is None:
        cfg = load_config()

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    soc_cfg     = cfg["socrata"]
    domain      = soc_cfg["domain"]
    app_token   = soc_cfg.get("app_token", "") or None
    dataset_ids = soc_cfg["dataset_ids"][data_type]
    page_size   = int(soc_cfg.get("page_size", 50000))
    max_retries = int(soc_cfg.get("max_retries", 3))
    nyc_counties = soc_cfg["nyc_facility_counties"]
    field_map   = soc_cfg["field_map"]
    years       = cfg["study"]["years"]

    age_group_map  = cfg.get("age_group_map", {})
    icd10_prefixes = cfg.get("icd10_prefixes", {})

    # 确定实际要请求的字段列表（根据字段映射表）
    fmap = field_map.get(data_type, {})

    for year in years:
        out_file = RAW_DIR / f"sparcs_{data_type}_{year}.csv"

        if out_file.exists():
            log.info("[CACHE] %s already exists, skipping.", out_file.name)
            continue

        ds_id = str(dataset_ids.get(year, "")).strip()
        if not ds_id:
            log.warning(
                "No Socrata dataset ID configured for %s year=%d. "
                "Provide it in config.yaml or place manual CSV at: %s",
                data_type, year, out_file
            )
            # 尝试本地 CSV
            local = RAW_DIR / f"sparcs_{data_type}_{year}.csv"
            if local.exists():
                log.info("  Local file found: %s", local.name)
            continue

        # 构建 SoQL WHERE 子句
        county_list = ", ".join(f"'{c}'" for c in nyc_counties)
        # 尝试两种可能的县字段名
        county_col = fmap.get("facility_county", "facility_county")

        # 年份过滤字段
        year_col = fmap.get("discharge_year", "discharge_year")

        where = (
            f"{county_col} in({county_list}) "
            f"AND {year_col} = '{year}'"
        )

        # 请求字段（取 field_map 中所有值 + 必要诊断字段）
        select_cols = list({v for v in fmap.values() if v})
        # 追加可能存在的备选诊断字段
        for alt in ["icd_diagnosis_code_1", "ccs_diagnosis_code"]:
            if alt not in select_cols:
                select_cols.append(alt)

        log.info("Fetching SPARCS %s year=%d from dataset=%s ...", data_type, year, ds_id)
        df_raw = _fetch_socrata_paged(
            domain=domain,
            dataset_id=ds_id,
            app_token=app_token,
            where_clause=where,
            select_cols=select_cols,
            page_size=page_size,
            max_retries=max_retries,
        )

        if df_raw.empty:
            log.warning(
                "No records returned for SPARCS %s year=%d. "
                "Check dataset ID or place manual CSV at: %s",
                data_type, year, out_file
            )
            continue

        log.info("  Raw records: %d rows, %d cols", *df_raw.shape)

        df_std = standardize_sparcs_df(
            df=df_raw,
            data_type=data_type,
            field_map=field_map,
            age_group_map=age_group_map,
            icd10_prefixes=icd10_prefixes,
            year=year,
        )

        df_std.to_csv(out_file, index=False)
        n_match = df_std["is_icd10_match"].sum() if "is_icd10_match" in df_std.columns else "?"
        log.info(
            "Saved → %s (%d rows; ICD-10 matches: %s)",
            out_file.name, len(df_std), n_match
        )
