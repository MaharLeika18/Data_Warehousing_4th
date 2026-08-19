from __future__ import annotations

import logging
import shutil
from pathlib import Path
 
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

import os
from dataclasses import dataclass, field
from dotenv import load_dotenv
import hashlib
import hmac
import sqlite3
from datetime import datetime, timezone

#--- Config 

@dataclass
class PipelineConfig:
    SQLITE_DB_PATH: str
    SOURCE_TABLE: str
    BATCH_SIZE: int
    SOURCE_SCHEMA: dict
    REQUIRED_ID_COLUMNS: list
    ORDER_STATUS_COLUMN: str
    ABANDONED_STATUS_VALUE: str
    COMPLETED_STATUS_VALUE: str
    NUMERIC_VALIDATION_COLUMNS: list
    CATEGORICAL_UPPERCASE_COLUMNS: list
    DATETIME_FORMATS: dict
    PII_COLUMNS: list
    PII_SALT: bytes
    DEDUP_KEY_COLUMNS: list
    METRICS_GROUP_BY: list
    PARTITION_COLUMNS: list
    DATALAKE_PATH: str
    PARQUET_COMPRESSION: str
    QUARANTINE_PATH: str
    QUARANTINE_FORMAT: str
    QUARANTINE_FILENAME: str
    QUARANTINE_FILENAME_PREFIX: str
    QUARANTINE_APPEND: bool
    LOG_PATH: str
    LOG_LEVEL: str

def get_config() -> PipelineConfig:
    load_dotenv()
    salt_str = os.environ.get("PII_SALT")
    if not salt_str:
        raise RuntimeError(
            "PII_SALT is not set. Copy .env.example to .env and set a salt (see README for more info)."
        )

    print("Press Enter on the following prompts to use default values.")

    return PipelineConfig(
        SQLITE_DB_PATH = input("Enter the path to your SQLite database file (.db): ") or "ladringan_saldua_prelim_project/data/orders_export.db",
        SOURCE_TABLE = input("Enter table name inside database file to be used: ") or "cart_events",
        BATCH_SIZE = int(input("Enter number of rows to pull per batch: ") or 5000),

        SOURCE_SCHEMA = {
            "order_id":             "int64",
            "session_id":           "object",
            "product_department":   "object",
            "item_price":           "float64",
            "cart_action_count":    "int64",
            "order_status":         "object",
            "order_date":           "datetime64[ns]"
        },

        REQUIRED_ID_COLUMNS = [
            "order_id", 
            "session_id", 
            "order_date"
        ],

        # PII = Personally Identifiable Info
        PII_COLUMNS = [     
            "session_id",
            "order_date"
        ],

        # Secret salt for HMAC hashing
        PII_SALT=salt_str.encode(),

        ORDER_STATUS_COLUMN = "order_status",
        ABANDONED_STATUS_VALUE = "ABANDONED",
        COMPLETED_STATUS_VALUE = "COMPLETED",

        NUMERIC_VALIDATION_COLUMNS = ["item_price", "cart_action_count"],
        CATEGORICAL_UPPERCASE_COLUMNS = ["order_status", "product_department"],
        DATETIME_FORMATS = {
            "order_timestamp": "%Y-%m-%d %H:%M:%S%z", 
        },

        DEDUP_KEY_COLUMNS = [
            "order_status", 
            "order_date"
        ],

        METRICS_GROUP_BY = [
            "product_department",
            # "item_price"
        ],

        # Hive-partitioned Parquet Output
        PARTITION_COLUMNS = ["product_department"],      
        DATALAKE_PATH = input("Enter the path to data lake: ") or "ladringan_saldua_prelim_project/data/analytics",
        PARQUET_COMPRESSION = "snappy",

        # Quarantine
        QUARANTINE_PATH = input("Enter the path to quarantine log: ") or "ladringan_saldua_prelim_project/quarantine",
        QUARANTINE_FORMAT = "csv",
        QUARANTINE_FILENAME = "null_product_ids.csv",
        QUARANTINE_FILENAME_PREFIX = "null_product_ids",
        QUARANTINE_APPEND = False, # TRUE for append mode FALSE for new file mode

        # Logging
        LOG_PATH = input("Enter the path to the pipeline log: ") or "ladringan_saldua_prelim_project/logs/cart_runs.log",
        LOG_LEVEL = "DEBUG", # INFO for default, DEBUG for verbose
    )

#--- Logging

def setup_logger(log_path: str, level: str) -> logging.Logger:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("cart_analytics")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear() 

    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(
        logging.Formatter("%(asctime)s | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
    )

    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(logging.Formatter("%(levelname)s | %(message)s"))
    logger.addHandler(stream_handler)

    return logger

def log_stage(logger, stage: str, in_count: int, out_count: int, quarantined: int) -> None:
    logger.info(
        f"[{stage}] in={in_count} out={out_count} quarantined={quarantined}"
    )

#--- Quarantine Procedure
 
class QuarantineManager:
    def __init__(self, cfg: "PipelineConfig", logger: logging.Logger):
        self.output_path = Path(cfg.QUARANTINE_PATH)
        self.output_format = cfg.QUARANTINE_FORMAT
        self.append = cfg.QUARANTINE_APPEND
        self.filename = cfg.QUARANTINE_FILENAME
        self.filename_prefix = cfg.QUARANTINE_FILENAME_PREFIX
        self.logger = logger
        self._batches: list[pd.DataFrame] = []

    def add(self, df: pd.DataFrame, reason: str, stage: str) -> None:
        if df.empty:
            return
        tagged = df.copy()
        tagged["_quarantine_reason"] = reason
        tagged["_quarantine_stage"] = stage
        tagged["_quarantine_ts"] = datetime.now(timezone.utc).isoformat()
        self._batches.append(tagged)
        self.logger.warning(f"Quarantined {len(df)} row(s) at stage='{stage}' reason='{reason}'")

    def flush(self) -> int:
        if not self._batches:
            return 0

        self.output_path.mkdir(parents=True, exist_ok=True)
        combined = pd.concat(self._batches, ignore_index=True)

        if self.append:
            file_path = self.output_path / self.filename
            file_exists = file_path.exists()

            if self.output_format == "parquet":
                if file_exists:
                    existing = pd.read_parquet(file_path)
                    combined = pd.concat([existing, combined], ignore_index=True)
                combined.to_parquet(file_path, index=False)
            else:
                combined.to_csv(
                    file_path,
                    mode="a" if file_exists else "w",
                    header=not file_exists,
                    index=False,
                )
        else:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S")
            ext = "parquet" if self.output_format == "parquet" else "csv"
            file_path = self.output_path / f"{self.filename_prefix}_{stamp}.{ext}"

            if self.output_format == "parquet":
                combined.to_parquet(file_path, index=False)
            else:
                combined.to_csv(file_path, index=False)

        self.logger.info(f"Flushed {len(combined)} quarantined row(s) to {file_path}")
        return len(combined)
    
#--- Extract

def fetch_source_batches(cfg: PipelineConfig, logger: logging.Logger):
    try:
        conn = sqlite3.connect(cfg.SQLITE_DB_PATH)
    except sqlite3.Error as e:
        logger.error(f"Failed to connect to SQLite DB at {cfg.SQLITE_DB_PATH}: {e}")
        raise

    try:
        query = f"SELECT * FROM {cfg.SOURCE_TABLE}"
        for batch in pd.read_sql_query(query, conn, chunksize=cfg.BATCH_SIZE):
            yield batch
    except (sqlite3.Error, pd.errors.DatabaseError) as e:
        logger.error(f"Failed reading from table '{cfg.SOURCE_TABLE}': {e}")
        raise
    finally:
        conn.close()

#--- Validation

def validate_batch_schema(df: pd.DataFrame, cfg: PipelineConfig, quarantine: QuarantineManager) -> pd.DataFrame:
    known_cols = [c for c in cfg.SOURCE_SCHEMA if c in df.columns]
    unknown_cols = [c for c in df.columns if c not in cfg.SOURCE_SCHEMA]
    if unknown_cols:
        logging.getLogger("pipeline").warning(f"Ignoring unexpected columns: {unknown_cols}")

    df = df[known_cols].copy()
    bad_row_mask = pd.Series(False, index=df.index)

    for col, dtype in cfg.SOURCE_SCHEMA.items():
        if col not in df.columns:
            continue
        try:
            if dtype == "datetime64[ns]":
                fmt = cfg.DATETIME_FORMATS.get(col)
                if fmt:
                    coerced = pd.to_datetime(df[col], format=fmt, errors="coerce")
                else:
                    coerced = pd.to_datetime(df[col], format="mixed", errors="coerce")
            elif dtype in ("int64", "float64"):
                coerced = pd.to_numeric(df[col], errors="coerce")
                if dtype == "int64":
                    coerced = coerced.astype("Int64")  # nullable int
            else:
                coerced = df[col].astype(dtype, errors="ignore")

            failed = coerced.isna() & df[col].notna()  # was real value, now failed coercion
            bad_row_mask |= failed  
            df[col] = coerced
        except (ValueError, TypeError) as e:
            logging.getLogger("pipeline").error(f"Column '{col}' coercion error: {e}")
            bad_row_mask |= True

    bad_rows = df[bad_row_mask]
    good_rows = df[~bad_row_mask]

    if not bad_rows.empty:
        quarantine.add(bad_rows, reason="schema_dtype_mismatch", stage="validate_schema")

    return good_rows

def standardize_text_columns(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    df = df.copy()

    object_cols = df.select_dtypes(include="object").columns
    for col in object_cols:
        df[col] = df[col].apply(lambda v: v.strip() if isinstance(v, str) else v)

    for col in cfg.CATEGORICAL_UPPERCASE_COLUMNS:
        if col in df.columns:
            df[col] = df[col].apply(lambda v: v.upper() if isinstance(v, str) else v)

    return df

def check_missing_identifiers(df: pd.DataFrame, cfg: PipelineConfig, quarantine: QuarantineManager) -> pd.DataFrame:
    id_cols = [c for c in cfg.REQUIRED_ID_COLUMNS if c in df.columns]
    if not id_cols:
        return df

    missing_mask = pd.Series(False, index=df.index)
    for col in id_cols:
        col_missing = df[col].isna() | (df[col].astype(str).str.strip() == "")
        missing_mask |= col_missing

    bad_rows = df[missing_mask]
    good_rows = df[~missing_mask]

    if not bad_rows.empty:
        quarantine.add(bad_rows, reason="missing_required_identifier", stage="check_missing_identifiers")

    return good_rows

def check_negative_values(df: pd.DataFrame, cfg: PipelineConfig, quarantine: QuarantineManager) -> pd.DataFrame:
    numeric_cols = [c for c in cfg.NUMERIC_VALIDATION_COLUMNS if c in df.columns]
    if not numeric_cols:
        return df

    bad_mask = pd.Series(False, index=df.index)
    for col in numeric_cols:
        bad_mask |= df[col] < 0

    bad_rows = df[bad_mask]
    good_rows = df[~bad_mask]

    if not bad_rows.empty:
        quarantine.add(bad_rows, reason="negative_numeric_value", stage="check_negative_values")

    return good_rows

#--- PII Masking

def hash_pii_value(value: str, salt: bytes) -> str:
    if pd.isna(value):
        return value
    return hmac.new(salt, str(value).encode("utf-8"), hashlib.sha256).hexdigest()

def mask_dataframe_pii(df: pd.DataFrame, cfg: PipelineConfig, quarantine: QuarantineManager) -> pd.DataFrame:
    df = df.copy()
    for col in cfg.PII_COLUMNS:
        if col not in df.columns:
            continue
        try:
            df[col] = df[col].apply(lambda v: hash_pii_value(v, cfg.PII_SALT))
        except Exception as e:
            failed_rows = df[df[col].notna()]
            quarantine.add(failed_rows, reason=f"pii_masking_failed:{col}:{e}", stage="mask_pii")
            df = df.drop(index=failed_rows.index)

    return df

#--- Transform

def deduplicate_session_actions(df: pd.DataFrame, cfg: PipelineConfig, logger: logging.Logger) -> pd.DataFrame:
    key_cols = [c for c in cfg.DEDUP_KEY_COLUMNS if c in df.columns]
    if not key_cols:
        return df

    before = len(df)
    df = df.drop_duplicates(subset=key_cols, keep="first")
    dropped = before - len(df)
    if dropped:
        logger.info(f"Dropped {dropped} duplicate session action(s)")
    return df

def flag_abandoned_products(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    df = df.copy()
    if cfg.ORDER_STATUS_COLUMN not in df.columns:
        df["is_abandoned"] = False
        return df
    df["is_abandoned"] = df[cfg.ORDER_STATUS_COLUMN] == cfg.ABANDONED_STATUS_VALUE
    return df

def compute_abandoned_product_metrics(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    if "is_abandoned" not in df.columns:
        return pd.DataFrame()

    metrics = (
        df[df["is_abandoned"]]
        .groupby(cfg.METRICS_GROUP_BY, dropna=False)
        .agg(abandoned_count=("is_abandoned", "size"))
        .reset_index()
    )
    return metrics

#--- Load

def enforce_output_dtypes(df: pd.DataFrame, cfg: PipelineConfig) -> pd.DataFrame:
    df = df.copy()
    for col, dtype in cfg.SOURCE_SCHEMA.items():
        if col not in df.columns:
            continue
        if dtype == "datetime64[ns]":
            fmt = cfg.DATETIME_FORMATS.get(col)
            if fmt:
                coerced = pd.to_datetime(df[col], format=fmt, errors="coerce")
            else:
                coerced = pd.to_datetime(df[col], format="mixed", errors="coerce")
        elif dtype == "int64":
            df[col] = df[col].astype("Int64")   # nullable int, avoids float upcasting on NaN
        elif dtype == "float64":
            df[col] = df[col].astype("float64")
        else:
            df[col] = df[col].astype(dtype)
    return df

def write_partitioned_parquet(df: pd.DataFrame, cfg: PipelineConfig, logger: logging.Logger, dataset_name: str) -> None:
    if df.empty:
        logger.warning(f"Skipping write for '{dataset_name}': DataFrame is empty")
        return

    partition_cols = [c for c in cfg.PARTITION_COLUMNS if c in df.columns]
    output_path = Path(cfg.DATALAKE_PATH) / dataset_name

    if output_path.exists():
        shutil.rmtree(output_path)
    output_path.mkdir(parents=True, exist_ok=True)

    try:
        table = pa.Table.from_pandas(df, preserve_index=False)
        pq.write_to_dataset(
            table,
            root_path=str(output_path),
            partition_cols=partition_cols if partition_cols else None,
            compression=cfg.PARQUET_COMPRESSION,
        )
        logger.info(f"Wrote {len(df)} row(s) to '{output_path}' partitioned by {partition_cols}")
    except (pa.ArrowInvalid, pa.ArrowTypeError, OSError) as e:
        logger.error(f"Failed writing dataset '{dataset_name}' to Parquet: {e}")
        raise

#--- Run

def run_pipeline() -> dict:
    cfg = get_config()
    logger = setup_logger(cfg.LOG_PATH, cfg.LOG_LEVEL)
    quarantine = QuarantineManager(cfg, logger)

    logger.info("Pipeline run started")

    all_clean_batches = []
    total_in = 0

    for batch_num, raw_batch in enumerate(fetch_source_batches(cfg, logger), start=1):
        total_in += len(raw_batch)
        try:
            df = validate_batch_schema(raw_batch, cfg, quarantine)
            df = standardize_text_columns(df, cfg)   
            df = check_missing_identifiers(df, cfg, quarantine)
            df = check_negative_values(df, cfg, quarantine)
            df = mask_dataframe_pii(df, cfg, quarantine)
            df = deduplicate_session_actions(df, cfg, logger)
            all_clean_batches.append(df)
            log_stage(logger, f"batch_{batch_num}", len(raw_batch), len(df), len(raw_batch) - len(df))
        except Exception as e:
            logger.exception(f"Batch {batch_num} failed unexpectedly: {e}")
            quarantine.add(raw_batch, reason=f"unhandled_batch_error:{e}", stage=f"batch_{batch_num}")

    if not all_clean_batches:
        logger.warning("No clean data survived validation — nothing to write")
        quarantine.flush()
        return {"total_in": total_in, "total_out": 0, "quarantined": total_in}

    clean_df = pd.concat(all_clean_batches, ignore_index=True)

    try:
        flagged_df = flag_abandoned_products(clean_df, cfg)
        flagged_df = enforce_output_dtypes(flagged_df, cfg)   
        metrics_df = compute_abandoned_product_metrics(flagged_df, cfg)

        write_partitioned_parquet(flagged_df, cfg, logger, dataset_name="events")
        write_partitioned_parquet(metrics_df, cfg, logger, dataset_name="abandoned_metrics")
    except Exception as e:
        logger.exception(f"Fatal error during transform/load stage: {e}")
        raise
    finally:
        quarantined_count = quarantine.flush()

    summary = {
        "total_in": total_in,
        "total_out": len(clean_df),
        "quarantined": quarantined_count,
    }
    logger.info(f"Pipeline run complete: {summary}")
    return summary

def main() -> None:
    summary = run_pipeline()
    print(f"Done. In: {summary['total_in']} | Out: {summary['total_out']} | Quarantined: {summary['quarantined']}")

if __name__ == "__main__":
    main()