"""
run_pipeline.py
===============
全流程调度脚本。按顺序执行：
  Step 1: EPA AQS 真实数据（公开批量文件，无需 API Key）
  Step 2: SPARCS 健康数据（优先 Socrata API；若未配置则用合成数据）
  Step 3: clean_merge.py → outputs/merged_for_GAM.csv

运行方式：
  python run_pipeline.py
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
sys.path.insert(0, str(SRC_DIR))

CONFIG_PATH = PROJECT_ROOT / "config.yaml"
(PROJECT_ROOT / "outputs").mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(PROJECT_ROOT / "outputs" / "pipeline.log",
                            mode="w", encoding="utf-8"),
    ],
)
log = logging.getLogger("pipeline")


def load_config() -> dict:
    with open(CONFIG_PATH, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def step1_epa(cfg: dict) -> None:
    log.info("=" * 60)
    log.info("STEP 1: Fetching EPA AQS bulk data (real, no API key needed)")
    log.info("=" * 60)
    import fetch_real_epa_bulk
    fetch_real_epa_bulk.run(cfg)


def step2_health(cfg: dict) -> None:
    log.info("=" * 60)
    log.info("STEP 2: Health data (SPARCS)")
    log.info("=" * 60)

    soc_token = cfg.get("socrata", {}).get("app_token", "")
    ds_ids = cfg.get("socrata", {}).get("dataset_ids", {})
    has_any_id = any(
        str(v).strip()
        for dt_ids in ds_ids.values()
        for v in dt_ids.values()
    )

    if has_any_id:
        log.info("Socrata dataset IDs found — attempting API fetch …")
        try:
            import fetch_sparcs_inpatient, fetch_sparcs_ed, fetch_sparcs_outpatient
            # These scripts call fetch_sparcs() with the configured IDs
        except Exception as exc:
            log.warning("Socrata fetch encountered error: %s", exc)
            log.info("Falling back to synthetic SPARCS data …")
            _run_synthetic(cfg)
    else:
        log.info("No Socrata credentials/IDs configured → generating synthetic SPARCS data.")
        _run_synthetic(cfg)


def _run_synthetic(cfg: dict) -> None:
    import generate_sparcs_synthetic
    generate_sparcs_synthetic.run(cfg)


def step3_merge(cfg: dict) -> None:
    log.info("=" * 60)
    log.info("STEP 3: Clean & merge → outputs/merged_for_GAM.csv")
    log.info("=" * 60)
    import clean_merge
    df_daily, df_monthly = clean_merge.merge_all(cfg)
    clean_merge.save_outputs(df_daily, df_monthly)
    clean_merge.print_summary(df_daily, df_monthly)


if __name__ == "__main__":
    cfg = load_config()

    step1_epa(cfg)
    step2_health(cfg)
    step3_merge(cfg)

    log.info("=" * 60)
    log.info("Pipeline complete.")
    log.info("Main output : outputs/merged_for_GAM.csv")
    log.info("Monthly backup: outputs/merged_for_GAM_monthly.csv")
    log.info("Full log    : outputs/pipeline.log")
    log.info("=" * 60)
