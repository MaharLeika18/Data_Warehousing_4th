import logging
import shutil
from pathlib import Path
 
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

#---

BASE_DIR = Path(__file__).resolve().parent
CSV_PATH = BASE_DIR / "data" / "orders_export.csv" 
LOG_DIR = BASE_DIR / "logs"
QUARANTINE_DIR = BASE_DIR / "quarantine"

LOG_DIR.mkdir(exist_ok=True)
QUARANTINE_DIR.mkdir(exist_ok=True)

#---

logger = logging.getLogger("cart_analytics")
logger.setLevel(logging.INFO)
logger.handlers.clear()

file_handler = logging.FileHandler(LOG_DIR / "cart_runs.log")
file_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))

logger.addHandler(file_handler)
logger.addHandler(stream_handler)

#---

def extract_orders() -> pd.DataFrame:
    try:
        df = pd.read_csv(CSV_PATH)
        logger.info(f"Extracted {len(df)} raw rows from {CSV_PATH.name}")
        return df
    except Exception as exc:
        logger.error(f"Extraction failed: {exc}")
        raise

#---

def quarantine_invalid_rows(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    try:
        missing_id = df["session_id"].isna() | (df["session_id"].astype(str).str.strip() == "")
        quarantined = df[missing_id].copy()
        quarantined["quarantine_reason"] = "missing_session_id"
        passed = df[~missing_id].copy()

        logger.info(f"Quarantined {len(quarantined)} rows, {len(passed)} passed")
        return passed, quarantined
    except Exception as exc:
        logger.error(f"Quarantine validation failed: {exc}")
        raise

#---

def clean_and_dedupe(df: pd.DataFrame) -> pd.DataFrame:
    try:
        df = df.copy()

        df = df[df["order_status"].astype(str).str.strip().str.upper() == "ABANDONED"]
        df["product_department"] = df["product_department"].astype(str).str.strip().str.lower()

        before = len(df)
        df = df[df["item_price"] >= 0]
        logger.info(f"Dropped {before - len(df)} rows with negative item_price")

        before = len(df)
        df = df.drop_duplicates(
            subset=["session_id", "product_department", "item_price", "cart_action_count", "order_date"]
        )
        logger.info(f"Removed {before - len(df)} duplicate session actions")

        logger.info(f"Cleaning complete: {len(df)} rows ready for aggregation")
        return df
    except Exception as exc:
        logger.error(f"Cleaning stage failed: {exc}")
        raise