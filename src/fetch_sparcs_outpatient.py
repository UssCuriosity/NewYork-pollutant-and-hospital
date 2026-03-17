"""
fetch_sparcs_outpatient.py
==========================
拉取纽约州 SPARCS Outpatient De-identified PUF（门诊去标识化公开数据）
覆盖 NYC 五县 × 2022–2024 年。

数据源：https://health.data.ny.gov/
数据集入口（Outpatient）：搜索 "SPARCS Outpatient De-identified"

本地 CSV 回退：
  data/raw/sparcs_outpatient_{year}.csv

运行：
  python src/fetch_sparcs_outpatient.py
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
    fetch_sparcs(data_type="outpatient", cfg=cfg)
    logging.getLogger(__name__).info("fetch_sparcs_outpatient.py completed.")
