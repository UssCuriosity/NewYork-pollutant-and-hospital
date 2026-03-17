"""
fetch_sparcs_inpatient.py
=========================
拉取纽约州 SPARCS Inpatient De-identified PUF（住院去标识化公开数据）
覆盖 NYC 五县 × 2022–2024 年。

数据源：https://health.data.ny.gov/
数据集入口（Inpatient）：搜索 "SPARCS Inpatient De-identified"
字段文档：https://www.health.ny.gov/statistics/sparcs/docs/

本地 CSV 回退：
  若 Socrata 数据集 ID 未配置或请求失败，
  请将手动下载的文件放到 data/raw/sparcs_inpatient_{year}.csv

运行：
  python src/fetch_sparcs_inpatient.py
"""

import logging
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "outputs" / "pipeline.log",
                            mode="a", encoding="utf-8"),
    ],
)

from _sparcs_base import fetch_sparcs, load_config  # noqa: E402

if __name__ == "__main__":
    cfg = load_config()
    fetch_sparcs(data_type="inpatient", cfg=cfg)
    logging.getLogger(__name__).info("fetch_sparcs_inpatient.py completed.")
