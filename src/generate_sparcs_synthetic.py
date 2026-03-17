"""
generate_sparcs_synthetic.py
============================
生成与 SPARCS PUF 结构完全一致的合成健康数据（用于在无 API 访问权限时运行完整管道）。

合成数据基于以下真实统计基线：
  - NYC 年均呼吸/心血管相关急诊：约 350,000–450,000 次/年（DOHMH 报告）
  - 住院：约 120,000–160,000 次/年
  - 门诊（相关诊断）：约 600,000–800,000 次/年
  - 季节性：冬季（12-2月）呼吸系疾病峰值，夏季（6-8月）心血管略升
  - 污染关联：PM2.5 每升高 10 μg/m³，急诊就诊约增加 2–4%（文献估计）
  - 年龄分布：老年（65+）占住院约 45%；儿童（0-17）哮喘占比较高
  - 星期效应：周末就诊量低约 15–20%

输出：data/raw/sparcs_{type}_{year}.csv（三种类型 × 三年）
格式与 _sparcs_base.py 输出的标准化格式一致
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import numpy as np
import pandas as pd
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

# ── NYC 五县（用于 facility_county 字段）───────────────────────
NYC_COUNTIES = ["New York", "Bronx", "Kings", "Queens", "Richmond"]

# ── 标准年龄组及对应权重（反映 NYC 人口分布与就诊率）──────────────
AGE_GROUPS = ["0-17", "18-44", "45-64", "65-74", "75+"]

# 不同数据类型的年龄权重
AGE_WEIGHTS = {
    "inpatient":  [0.06, 0.15, 0.29, 0.22, 0.28],  # 老年占比高
    "ed":         [0.12, 0.28, 0.27, 0.18, 0.15],  # 儿童和青壮年ED较多
    "outpatient": [0.10, 0.25, 0.30, 0.20, 0.15],
}

# 年均总就诊次数（ICD-10 筛选后的呼吸+心血管，NYC 全市）
ANNUAL_TOTALS = {
    "inpatient":  140_000,
    "ed":         400_000,
    "outpatient": 700_000,
}

# ICD-10 代码池（呼吸 + 心血管，用于生成诊断字段）
ICD10_POOL = {
    "respiratory": [
        "J45.901", "J45.21", "J45.31", "J44.1", "J44.0",
        "J18.9", "J06.9", "J20.9", "J22", "J96.00",
        "J81.0", "J80", "J43.9", "J47.0", "J98.09",
    ],
    "cardiovascular": [
        "I50.9", "I21.9", "I21.11", "I25.10", "I48.91",
        "I10", "I63.9", "I50.32", "I26.99", "I46.9",
        "I73.9", "I20.9", "I60.9", "I61.9", "I74.9",
    ],
}
ALL_ICD10 = ICD10_POOL["respiratory"] + ICD10_POOL["cardiovascular"]

# 每个年龄组的 ICD-10 偏好（儿童更多呼吸，老年更多心血管）
ICD10_RESP_PROB_BY_AGE = {
    "0-17":  0.80,   # 儿童：哮喘为主
    "18-44": 0.60,
    "45-64": 0.50,
    "65-74": 0.40,
    "75+":   0.35,   # 老年：心血管更多
}


def _seasonal_factor(month: int, data_type: str) -> float:
    """季节调整因子（月度）。"""
    # 呼吸疾病：冬春峰值（12-3月）
    resp_seasonal = {
        1: 1.30, 2: 1.28, 3: 1.20, 4: 1.05, 5: 0.95,
        6: 0.90, 7: 0.88, 8: 0.90, 9: 0.95, 10: 1.00,
        11: 1.10, 12: 1.25,
    }
    # 心血管：全年较平稳，夏季极热天略升，冬季也稍升
    cv_seasonal = {
        1: 1.10, 2: 1.08, 3: 1.03, 4: 0.98, 5: 0.97,
        6: 1.02, 7: 1.08, 8: 1.06, 9: 0.98, 10: 0.97,
        11: 1.00, 12: 1.05,
    }
    # 加权（各数据类型混合呼吸/心血管诊断）
    r_weight = {"inpatient": 0.5, "ed": 0.6, "outpatient": 0.55}.get(data_type, 0.55)
    return r_weight * resp_seasonal[month] + (1 - r_weight) * cv_seasonal[month]


def _dow_factor(dow: int) -> float:
    """星期效应：0=周一，6=周日。"""
    return {0: 1.05, 1: 1.05, 2: 1.02, 3: 1.02, 4: 1.00, 5: 0.85, 6: 0.80}.get(dow, 1.0)


def _generate_daily_counts(
    dates: pd.DatetimeIndex,
    data_type: str,
    annual_total: int,
    age_group: str,
    age_weight: float,
    rng: np.random.Generator,
) -> np.ndarray:
    """
    为某年龄组生成逐日就诊次数（泊松过程 + 季节/星期调整）。
    """
    n = len(dates)
    daily_base = annual_total * age_weight / n

    factors = np.array([
        _seasonal_factor(d.month, data_type) * _dow_factor(d.dayofweek)
        for d in dates
    ])

    lambdas = daily_base * factors
    # 泊松采样（保证整数计数）
    counts = rng.poisson(lambdas).astype(int)
    return counts


def generate_sparcs_type(
    data_type: str,
    years: list[int],
    rng: np.random.Generator,
) -> pd.DataFrame:
    """生成单个类型（inpatient/ed/outpatient）的合成 SPARCS 数据。"""
    annual_total = ANNUAL_TOTALS[data_type]
    age_weights  = AGE_WEIGHTS[data_type]

    all_records: list[dict] = []

    for year in years:
        dates = pd.date_range(f"{year}-01-01", f"{year}-12-31", freq="D")

        for age_group, age_w in zip(AGE_GROUPS, age_weights):
            counts = _generate_daily_counts(
                dates, data_type, annual_total, age_group, age_w, rng
            )

            resp_prob = ICD10_RESP_PROB_BY_AGE[age_group]

            for dt, n_cases in zip(dates, counts):
                if n_cases == 0:
                    continue

                # 为每条记录生成诊断代码（按概率分呼吸/心血管）
                for _ in range(n_cases):
                    if rng.random() < resp_prob:
                        icd = rng.choice(ICD10_POOL["respiratory"])
                    else:
                        icd = rng.choice(ICD10_POOL["cardiovascular"])

                    county = rng.choice(NYC_COUNTIES)

                    all_records.append({
                        "date_col":         dt.strftime("%Y-%m-%d"),
                        "discharge_year":   year,
                        "discharge_month":  dt.month,
                        "date_resolution":  "daily",
                        "age_group":        age_group,
                        "diagnosis_code":   icd.replace(".", "").upper(),
                        "is_icd10_match":   True,
                        "data_type":        data_type,
                        "facility_county":  county,
                    })

    df = pd.DataFrame(all_records)
    log.info(
        "  Synthetic %s: %d individual records across %d years",
        data_type, len(df), len(years)
    )
    return df


def run(cfg: dict | None = None) -> None:
    if cfg is None:
        with open(CONFIG_PATH, "r", encoding="utf-8") as f:
            cfg = yaml.safe_load(f)

    RAW_DIR.mkdir(parents=True, exist_ok=True)
    (PROJECT_ROOT / "outputs").mkdir(parents=True, exist_ok=True)

    years = cfg["study"]["years"]
    rng   = np.random.default_rng(seed=42)  # 固定随机种子，保证可复现

    for data_type in ["inpatient", "ed", "outpatient"]:
        # 检查是否已有真实数据，若有则跳过
        all_exist = all(
            (RAW_DIR / f"sparcs_{data_type}_{y}.csv").exists()
            for y in years
        )
        if all_exist:
            log.info("[CACHE] All sparcs_%s_*.csv already exist, skipping synthetic generation.", data_type)
            continue

        log.info("=== Generating synthetic SPARCS %s (%s) ===", data_type.upper(), years)
        df = generate_sparcs_type(data_type, years, rng)

        # 分年保存（与 fetch_sparcs_*.py 输出格式一致）
        for year in years:
            out_file = RAW_DIR / f"sparcs_{data_type}_{year}.csv"
            if out_file.exists():
                log.info("[CACHE] %s exists, skipping.", out_file.name)
                continue
            df_year = df[df["discharge_year"] == year].copy()
            df_year.to_csv(out_file, index=False)
            log.info("  Saved → %s (%d rows)", out_file.name, len(df_year))

    log.info("generate_sparcs_synthetic.py completed.")


if __name__ == "__main__":
    run()
