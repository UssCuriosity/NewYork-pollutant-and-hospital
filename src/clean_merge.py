"""
clean_merge.py
==============
全流程清洗与整合管道：

  Step 1: 处理 EPA AQS 数据
    - 读取 data/raw/epa_aqs_{param}_{year}.csv
    - 五县站点日均加权平均（权重 = observation_count）
    - 构建完整日历（2022-01-01 → 2024-12-31）并线性插值填充短期缺口

  Step 2: 处理 SPARCS 健康数据
    - 读取 data/raw/sparcs_{type}_{year}.csv（inpatient/ed/outpatient）
    - ICD-10 白名单过滤（config.yaml icd10_prefixes）
    - 年龄分组标准化
    - 时间粒度修正（daily → 直接用；monthly → 均摊；annual → 警告并月聚合）

  Step 3: 合并
    - 以 EPA 全日历 × 6 年龄组为骨架（left join）
    - 添加辅助字段：dow, month, year, is_holiday
    - 输出 outputs/merged_for_GAM.csv（日级，含 date_resolution 标记）
    - 附加输出 outputs/merged_for_GAM_monthly.csv（月级聚合备用）

运行：
  python src/clean_merge.py
"""

from __future__ import annotations

import logging
import sys
from calendar import monthrange
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
import yaml

# ── 项目路径 ─────────────────────────────────────────────────
PROJECT_ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH  = PROJECT_ROOT / "config.yaml"
RAW_DIR      = PROJECT_ROOT / "data" / "raw"
PROC_DIR     = PROJECT_ROOT / "data" / "processed"
OUT_DIR      = PROJECT_ROOT / "outputs"

for d in (PROC_DIR, OUT_DIR):
    d.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(OUT_DIR / "pipeline.log", mode="a", encoding="utf-8"),
    ],
)
log = logging.getLogger(__name__)


# ── 配置 ─────────────────────────────────────────────────────

def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


# ═══════════════════════════════════════════════════════════════
# STEP 1: EPA AQS 数据清洗
# ═══════════════════════════════════════════════════════════════

def load_epa_aqs(cfg: dict) -> pd.DataFrame:
    """
    读取 data/raw/epa_aqs_{param}_{year}.csv，
    计算五县日均加权平均浓度，
    返回 DataFrame: date(str), pm25, no2, so2。
    """
    years      = cfg["study"]["years"]
    params_map = cfg["epa_aqs"]["parameters"]

    frames_by_param: dict[str, list[pd.DataFrame]] = {p: [] for p in params_map}

    for year in years:
        for param_name in params_map:
            fpath = RAW_DIR / f"epa_aqs_{param_name}_{year}.csv"
            if not fpath.exists():
                log.warning("Missing EPA file: %s", fpath.name)
                continue
            df = pd.read_csv(fpath, low_memory=False)
            if df.empty:
                continue
            if "date" not in df.columns or "arithmetic_mean" not in df.columns:
                log.warning("Expected columns not found in %s", fpath.name)
                continue
            df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.strftime("%Y-%m-%d")
            df["arithmetic_mean"] = pd.to_numeric(df["arithmetic_mean"], errors="coerce")
            df["observation_count"] = pd.to_numeric(
                df.get("observation_count", pd.Series(1, index=df.index)), errors="coerce"
            ).fillna(1)
            frames_by_param[param_name].append(df)

    # 加权日均
    aqs_dfs: dict[str, pd.DataFrame] = {}
    for param_name, frames in frames_by_param.items():
        if not frames:
            log.warning("No data loaded for param=%s; will be NaN in output.", param_name)
            continue
        df_all = pd.concat(frames, ignore_index=True)
        df_all = df_all.dropna(subset=["date", "arithmetic_mean"])

        # 按日聚合：加权平均
        def weighted_mean(grp: pd.DataFrame) -> float:
            w = grp["observation_count"].clip(lower=1)
            return float(np.average(grp["arithmetic_mean"], weights=w))

        df_daily = (
            df_all.groupby("date")
            .apply(weighted_mean, include_groups=False)
            .reset_index(name=param_name)
        )
        aqs_dfs[param_name] = df_daily
        log.info("EPA %s: %d daily values loaded.", param_name.upper(), len(df_daily))

    if not aqs_dfs:
        log.error("No EPA data loaded at all. Check data/raw/.")
        return pd.DataFrame(columns=["date", "pm25", "no2", "so2"])

    # 合并三种污染物
    start = pd.Timestamp(f"{cfg['study']['start_year']}-01-01")
    end   = pd.Timestamp(f"{cfg['study']['end_year']}-12-31")
    all_dates = pd.DataFrame(
        {"date": pd.date_range(start, end).strftime("%Y-%m-%d")}
    )

    df_aqs = all_dates.copy()
    for param_name, df_p in aqs_dfs.items():
        df_aqs = df_aqs.merge(df_p, on="date", how="left")

    # 确保三列都存在
    for p in ["pm25", "no2", "so2"]:
        if p not in df_aqs.columns:
            df_aqs[p] = np.nan

    # 线性插值填充短期缺口（≤3 天连续缺失）
    for p in ["pm25", "no2", "so2"]:
        df_aqs[p] = (
            df_aqs[p]
            .interpolate(method="linear", limit=3, limit_direction="both")
        )
        n_null = df_aqs[p].isna().sum()
        if n_null > 0:
            log.warning("  %s: %d dates still missing after interpolation.", p.upper(), n_null)

    log.info("EPA AQS: date range %s to %s, %d rows.", df_aqs["date"].min(), df_aqs["date"].max(), len(df_aqs))
    return df_aqs


# ═══════════════════════════════════════════════════════════════
# STEP 2: SPARCS 健康数据清洗
# ═══════════════════════════════════════════════════════════════

def _load_single_sparcs(fpath: Path, data_type: str) -> pd.DataFrame:
    """读取单个 SPARCS 标准化 CSV 文件。"""
    if not fpath.exists():
        log.warning("Missing SPARCS file: %s", fpath.name)
        return pd.DataFrame()
    df = pd.read_csv(fpath, low_memory=False)
    if df.empty:
        return df
    df["source_file"] = fpath.name
    log.info("  Loaded %s: %d rows", fpath.name, len(df))
    return df


def _expand_monthly_to_daily(
    df_month: pd.DataFrame, year: int, month: int
) -> pd.DataFrame:
    """
    将月级聚合数据均摊到该月每一天。
    输入：含 (age_group, data_type, n_cases) 的 DataFrame（单月汇总）
    输出：为每天复制一行，n_cases = 月总数 / days_in_month
    """
    days = monthrange(year, month)[1]
    dates = [f"{year}-{month:02d}-{d:02d}" for d in range(1, days + 1)]
    df_month = df_month.copy()
    df_month["n_cases_daily"] = df_month["n_cases"] / days
    expanded = pd.concat(
        [df_month.assign(date=dt) for dt in dates],
        ignore_index=True
    )
    return expanded


def load_sparcs_health(cfg: dict) -> pd.DataFrame:
    """
    读取三类 SPARCS 数据，ICD-10 过滤后，
    返回 date × age_group × data_type 维度的计数表。

    列：date(str), age_group, data_type, n_cases, date_resolution
    """
    years      = cfg["study"]["years"]
    data_types = ["inpatient", "ed", "outpatient"]
    all_frames: list[pd.DataFrame] = []

    for dt in data_types:
        for year in years:
            fpath = RAW_DIR / f"sparcs_{dt}_{year}.csv"
            df = _load_single_sparcs(fpath, dt)
            if df.empty:
                continue

            # ICD-10 过滤（is_icd10_match 字段由 _sparcs_base 已标注）
            if "is_icd10_match" in df.columns:
                df = df[df["is_icd10_match"] == True].copy()
            else:
                # 若字段缺失，重新过滤
                icd_prefixes = cfg.get("icd10_prefixes", {})
                all_pfx = [p for g in icd_prefixes.values() for p in g]
                if "diagnosis_code" in df.columns:
                    mask = df["diagnosis_code"].astype(str).apply(
                        lambda c: any(c.upper().startswith(p) for p in all_pfx)
                    )
                    df = df[mask].copy()

            if df.empty:
                log.info("  %s %d: no ICD-10 matching records.", dt, year)
                continue

            # 年龄分组缺失填充
            if "age_group" not in df.columns:
                df["age_group"] = "Unknown"
            df["age_group"] = df["age_group"].fillna("Unknown")

            # 时间粒度处理
            if "date_resolution" not in df.columns:
                df["date_resolution"] = "unknown"

            resolution = df["date_resolution"].mode()[0] if not df["date_resolution"].empty else "unknown"

            if resolution == "daily" and "date_col" in df.columns:
                # 逐日计数
                df_count = (
                    df.groupby(["date_col", "age_group"])
                    .size()
                    .reset_index(name="n_cases")
                    .rename(columns={"date_col": "date"})
                )
                df_count["date_resolution"] = "daily"
                df_count["data_type"] = dt

            elif resolution == "monthly":
                # 月级 → 均摊到每日
                if "discharge_year" not in df.columns or "discharge_month" not in df.columns:
                    log.warning("  %s %d: monthly resolution but year/month cols missing.", dt, year)
                    continue

                df["discharge_year"]  = pd.to_numeric(df["discharge_year"],  errors="coerce").astype("Int64")
                df["discharge_month"] = pd.to_numeric(df["discharge_month"], errors="coerce").astype("Int64")
                df = df.dropna(subset=["discharge_year", "discharge_month"])

                # 月级聚合
                df_month_agg = (
                    df.groupby(["discharge_year", "discharge_month", "age_group"])
                    .size()
                    .reset_index(name="n_cases")
                )
                df_month_agg["data_type"] = dt

                # 逐月均摊到每日
                expanded_parts: list[pd.DataFrame] = []
                for _, row in df_month_agg.iterrows():
                    y = int(row["discharge_year"])
                    m = int(row["discharge_month"])
                    tmp = pd.DataFrame([{
                        "age_group": row["age_group"],
                        "data_type": dt,
                        "n_cases": row["n_cases"],
                    }])
                    expanded_parts.append(_expand_monthly_to_daily(tmp, y, m))

                if not expanded_parts:
                    continue

                df_count = pd.concat(expanded_parts, ignore_index=True)
                df_count = df_count.rename(columns={"n_cases_daily": "n_cases"})
                if "n_cases" not in df_count.columns and "n_cases_daily" in df_count.columns:
                    df_count = df_count.rename(columns={"n_cases_daily": "n_cases"})
                df_count["date_resolution"] = "monthly"
                log.warning(
                    "  %s %d: SPARCS only has monthly data. "
                    "Counts evenly distributed across days (date_resolution=monthly).",
                    dt, year
                )

            else:
                # 年级或未知粒度
                log.warning(
                    "  %s %d: date_resolution='%s'. "
                    "Aggregating to monthly output only (see merged_for_GAM_monthly.csv).",
                    dt, year, resolution
                )
                if "discharge_year" in df.columns:
                    df_count = (
                        df.groupby(["discharge_year", "age_group"])
                        .size()
                        .reset_index(name="n_cases")
                    )
                    df_count["date"] = df_count["discharge_year"].astype(str) + "-01-01"
                    df_count["data_type"] = dt
                    df_count["date_resolution"] = "annual"
                else:
                    continue

            all_frames.append(df_count)

    if not all_frames:
        log.error("No SPARCS health data loaded. Output will have NaN health columns.")
        return pd.DataFrame(columns=["date", "age_group", "data_type", "n_cases", "date_resolution"])

    df_health = pd.concat(all_frames, ignore_index=True)

    # 统一日期格式
    df_health["date"] = pd.to_datetime(df_health["date"], errors="coerce").dt.strftime("%Y-%m-%d")
    df_health = df_health.dropna(subset=["date"])
    df_health["n_cases"] = pd.to_numeric(df_health["n_cases"], errors="coerce").fillna(0)

    log.info("SPARCS health: %d rows loaded (after ICD-10 filter + time handling).", len(df_health))
    return df_health


# ═══════════════════════════════════════════════════════════════
# STEP 3: 构建骨架 × 合并
# ═══════════════════════════════════════════════════════════════

def _build_holidays_set(start_year: int, end_year: int) -> set[str]:
    """返回美国联邦节假日日期字符串集合。"""
    try:
        import holidays as hol_lib
        us_holidays: set[str] = set()
        for y in range(start_year, end_year + 1):
            for dt in hol_lib.US(years=y).keys():
                us_holidays.add(str(dt))
        return us_holidays
    except ImportError:
        log.warning("'holidays' package not installed; is_holiday will be 0.")
        return set()


def build_skeleton(cfg: dict, age_groups: list[str]) -> pd.DataFrame:
    """
    构建 日期 × 年龄组 的完整骨架 DataFrame。
    """
    start = pd.Timestamp(f"{cfg['study']['start_year']}-01-01")
    end   = pd.Timestamp(f"{cfg['study']['end_year']}-12-31")
    dates = pd.date_range(start, end).strftime("%Y-%m-%d").tolist()

    rows = [{"date": d, "age_group": ag} for d in dates for ag in age_groups]
    return pd.DataFrame(rows)


def pivot_health(df_health: pd.DataFrame) -> pd.DataFrame:
    """
    将长表（date, age_group, data_type, n_cases）
    转为宽表（date, age_group, n_inpatient, n_ed, n_outpatient）。
    """
    if df_health.empty:
        return pd.DataFrame(columns=["date", "age_group",
                                     "n_inpatient", "n_ed", "n_outpatient",
                                     "date_resolution"])

    # 先聚合（防止重复行）
    df_agg = (
        df_health.groupby(["date", "age_group", "data_type"], dropna=False)["n_cases"]
        .sum()
        .reset_index()
    )

    # 保留最高粒度标记（daily > monthly > annual）
    if "date_resolution" in df_health.columns:
        res_order = {"daily": 0, "monthly": 1, "annual": 2, "unknown": 3}
        df_res = (
            df_health.groupby(["date", "age_group", "data_type"])["date_resolution"]
            .apply(lambda s: min(s, key=lambda v: res_order.get(v, 99)))
            .reset_index()
        )
        df_agg = df_agg.merge(df_res, on=["date", "age_group", "data_type"], how="left")
    else:
        df_agg["date_resolution"] = "unknown"

    # 透视
    df_wide = df_agg.pivot_table(
        index=["date", "age_group", "date_resolution"],
        columns="data_type",
        values="n_cases",
        aggfunc="sum",
        fill_value=0,
    ).reset_index()

    # 规范化列名
    df_wide.columns.name = None
    rename_map = {
        "inpatient":  "n_inpatient",
        "ed":         "n_ed",
        "outpatient": "n_outpatient",
    }
    df_wide = df_wide.rename(columns=rename_map)
    for col in ["n_inpatient", "n_ed", "n_outpatient"]:
        if col not in df_wide.columns:
            df_wide[col] = 0

    # 每个 (date, age_group) 只保留一行
    df_wide = (
        df_wide.groupby(["date", "age_group"], as_index=False)
        .agg({
            "n_inpatient":    "sum",
            "n_ed":           "sum",
            "n_outpatient":   "sum",
            "date_resolution": "first",
        })
    )
    return df_wide


def merge_all(cfg: dict) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    主合并函数，返回 (df_daily, df_monthly)。
    """
    age_groups = cfg.get("age_groups_ordered", ["0-17", "18-44", "45-64", "65-74", "75+"])

    # ── 加载数据 ──
    log.info("=== Loading EPA AQS data ===")
    df_aqs = load_epa_aqs(cfg)

    log.info("=== Loading SPARCS health data ===")
    df_health = load_sparcs_health(cfg)

    # ── 骨架 ──
    log.info("=== Building date × age_group skeleton ===")
    df_skeleton = build_skeleton(cfg, age_groups)

    # ── 污染物合并到骨架 ──
    df_base = df_skeleton.merge(df_aqs, on="date", how="left")

    # ── 健康数据宽表 ──
    df_health_wide = pivot_health(df_health)

    # ── 最终合并 ──
    df_daily = df_base.merge(df_health_wide, on=["date", "age_group"], how="left")

    # 健康结局计数缺失填 0（日期存在但无就诊记录）
    for col in ["n_inpatient", "n_ed", "n_outpatient"]:
        if col not in df_daily.columns:
            df_daily[col] = 0
        else:
            df_daily[col] = df_daily[col].fillna(0).astype(int)

    if "date_resolution" not in df_daily.columns:
        df_daily["date_resolution"] = "daily"
    else:
        df_daily["date_resolution"] = df_daily["date_resolution"].fillna("daily")

    # ── 辅助时间字段 ──
    dt_col = pd.to_datetime(df_daily["date"])
    df_daily["dow"]   = dt_col.dt.dayofweek   # 0=Monday
    df_daily["month"] = dt_col.dt.month
    df_daily["year"]  = dt_col.dt.year

    holidays_set = _build_holidays_set(
        cfg["study"]["start_year"], cfg["study"]["end_year"]
    )
    df_daily["is_holiday"] = df_daily["date"].isin(holidays_set).astype(int)

    # ── 列排序 ──
    ordered_cols = [
        "date", "age_group",
        "n_inpatient", "n_ed", "n_outpatient",
        "pm25", "no2", "so2",
        "dow", "month", "year", "is_holiday",
        "date_resolution",
    ]
    df_daily = df_daily[[c for c in ordered_cols if c in df_daily.columns]]

    # ── 月级聚合（备用） ──
    df_monthly = _build_monthly(df_daily)

    return df_daily, df_monthly


def _build_monthly(df_daily: pd.DataFrame) -> pd.DataFrame:
    """
    将日级表聚合为月级表，供月级 DLNM 使用。
    健康计数：求和；污染浓度：月均值。
    """
    df = df_daily.copy()
    df["year_month"] = (
        pd.to_datetime(df["date"]).dt.to_period("M").astype(str)
    )
    agg_dict: dict = {
        "n_inpatient":  "sum",
        "n_ed":         "sum",
        "n_outpatient": "sum",
        "pm25":         "mean",
        "no2":          "mean",
        "so2":          "mean",
        "is_holiday":   "sum",   # 该月节假日天数
    }
    agg_dict = {k: v for k, v in agg_dict.items() if k in df.columns}
    df_m = (
        df.groupby(["year_month", "age_group"])
        .agg(agg_dict)
        .reset_index()
    )
    df_m = df_m.rename(columns={
        "year_month":   "month_period",
        "is_holiday":   "holiday_days_in_month",
    })
    return df_m


# ═══════════════════════════════════════════════════════════════
# 输出与验收摘要
# ═══════════════════════════════════════════════════════════════

def print_summary(df_daily: pd.DataFrame, df_monthly: pd.DataFrame) -> None:
    log.info("=== OUTPUT SUMMARY ===")
    log.info("Daily table: %d rows × %d cols", *df_daily.shape)
    log.info("  Date range: %s  →  %s", df_daily["date"].min(), df_daily["date"].max())
    log.info("  Age groups: %s", sorted(df_daily["age_group"].unique().tolist()))
    for col in ["pm25", "no2", "so2"]:
        if col in df_daily.columns:
            log.info("  %s: mean=%.2f, null=%d",
                     col.upper(), df_daily[col].mean(), df_daily[col].isna().sum())
    for col in ["n_inpatient", "n_ed", "n_outpatient"]:
        if col in df_daily.columns:
            log.info("  %s: total=%d", col, int(df_daily[col].sum()))
    resolutions = df_daily["date_resolution"].value_counts().to_dict() if "date_resolution" in df_daily.columns else {}
    log.info("  date_resolution counts: %s", resolutions)
    log.info("Monthly table: %d rows × %d cols", *df_monthly.shape)


def save_outputs(df_daily: pd.DataFrame, df_monthly: pd.DataFrame) -> None:
    daily_path   = OUT_DIR / "merged_for_GAM.csv"
    monthly_path = OUT_DIR / "merged_for_GAM_monthly.csv"

    df_daily.to_csv(daily_path, index=False)
    log.info("Saved → %s", daily_path)

    df_monthly.to_csv(monthly_path, index=False)
    log.info("Saved → %s", monthly_path)

    # 中间产物
    df_daily.to_csv(PROC_DIR / "daily_merged_intermediate.csv", index=False)


# ── 入口 ─────────────────────────────────────────────────────

if __name__ == "__main__":
    cfg = load_config()
    df_daily, df_monthly = merge_all(cfg)
    save_outputs(df_daily, df_monthly)
    print_summary(df_daily, df_monthly)
    log.info("clean_merge.py completed. Final output: outputs/merged_for_GAM.csv")
