from __future__ import annotations
import pandas as pd
import numpy as np
import os
import json
import sqlite3
import pymongo
from pathlib import Path
import logging
import shutil
from pathlib import Path
from dataclasses import dataclass, field
from datetime import datetime, timezone
import time
from dotenv import load_dotenv
import re

@dataclass
class PipelineConfig:
    LANDING_ZONE_PATH: str
    CSV_GLOB_PATTERN: str
    JSON_GLOB_PATTERN:str
    PROCESSED_PATH: str
    QUARANTINE_FILES_PATH: str  # not sure if we still need this
    SQLITE_OUTPUT_PATH: str
    SQLITE_OUTPUT_TABLE: str
    MONGO_URI: str
    MONGO_DB_NAME: str
    MONGO_COLLECTION_NAME: str
    LOG_PATH: str

def get_config() -> PipelineConfig:
    load_dotenv()
    mongo_uri = os.environ.get("MONGO_URI")
    if not mongo_uri:
        raise RuntimeError(
            "MONGO_URI is not set. Copy .env.example to .env and set your MongoDB connection string."
        )
    
    return PipelineConfig(
        LANDING_ZONE_PATH = "lesson_4/landing_zone",
        CSV_GLOB_PATTERN = "*.csv",
        JSON_GLOB_PATTERN = "*.json",
        PROCESSED_PATH = "lesson_4/landing_zone/_processed",
        QUARANTINE_FILES_PATH = "lesson_4/quarantine",
        SQLITE_OUTPUT_PATH = "lesson_4/data/SQLite",
        SQLITE_OUTPUT_TABLE = "fact_sales",
        MONGO_URI = mongo_uri,
        MONGO_DB_NAME = "warehouse_db",
        MONGO_COLLECTION_NAME = "sales_documents",
        LOG_PATH= "lesson_4/logs",
    )

# ------ Logging ------
def setup_logger(log_path: str, level: str = "info") -> logging.Logger:
    Path(log_path).parent.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("pipeline")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    file_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")     # not asked for but keeping the time logging for debugging
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)

    console_fmt = logging.Formatter("%(message)s")
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)
    return logger

def log_stage(logger: logging.Logger, stage: str, message: str, level: str = "info") -> None:
    log_fn = getattr(logger, level.lower(), logger.info)
    log_fn(f"[{stage}] {message}")

# ------ Connect to DBs ------

# SQLite


# MongoDB


# ------ Extract ------
# read raw csv and json logs from local landing zone directory
def discover_landing_zone_files(cfg: PipelineConfig) -> dict[str, list[Path]]:
    landing_dir = Path(cfg.LANDING_ZONE_PATH)
    return {
        "csv": sorted(landing_dir.glob(cfg.CSV_GLOB_PATTERN)),
        "json": sorted(landing_dir.glob(cfg.JSON_GLOB_PATTERN)),
    }

def read_csv_file(
    file_path: Path, cfg: PipelineConfig, logger: logging.Logger
) -> pd.DataFrame | None:
    try:
        df = pd.read_csv(
            file_path,
            encoding="utf-8",

            on_bad_lines="warn",
            skip_blank_lines=True,
        )
        if df.empty:
            return None
        df["_source_file"] = file_path.name
        return df

    except UnicodeDecodeError as e:
        log_stage(logger, "EXTRACT", f"Encoding error in {file_path.name}, retrying with latin-1: {e}", level="warning")
        try:
            df = pd.read_csv(file_path, encoding="latin-1", on_bad_lines="warn")
            df["_source_file"] = file_path.name
            return df
        except Exception as e2:
            log_stage(logger, "EXTRACT", f"Failed to read {file_path.name} even with fallback encoding: {e2}", level="error")
            return None

    except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        log_stage(logger, "EXTRACT", f"Failed to parse {file_path.name}: {e}", level="error")
        return None

def read_json_file(
    file_path: Path, cfg: PipelineConfig, logger: logging.Logger
) -> list[dict] | None:

    try:
        raw_text = file_path.read_text(encoding="utf-8")
    except UnicodeDecodeError as e:
        logger.error(f"Failed to read {file_path.name}: {e}")
        return None

    # try standard JSON array 
    try:
        data = json.loads(raw_text)
        records = data if isinstance(data, list) else [data]
        for r in records:
            r["_source_file"] = file_path.name
        return records
    except json.JSONDecodeError:
        pass

    # try NDJSON
    records = []
    malformed_lines = 0
    for i, line in enumerate(raw_text.splitlines(), start=1):
        line = line.strip()
        if not line:
            continue
        try:
            record = json.loads(line)
            record["_source_file"] = file_path.name
            record["_source_line"] = i
            records.append(record)
        except json.JSONDecodeError:
            malformed_lines += 1

    if malformed_lines:
        logger.warning(f"{file_path.name}: {malformed_lines} malformed JSON line(s) skipped")

    if not records:
        logger.error(f"No valid JSON records found in {file_path.name}")
        return None

    return records

# ingest into pd dataframe
def fetch_source_batches(cfg: PipelineConfig, logger: logging.Logger):
    logger = setup_logger(cfg.LOG_PATH)
    files = discover_landing_zone_files(cfg)

    csv_frames = [
        df for path in files["csv"]
        if (df := read_csv_file(path, cfg, logger)) is not None
    ]
    combined_csv = pd.concat(csv_frames, ignore_index=True) if csv_frames else pd.DataFrame()

    json_records: list[dict] = []
    for path in files["json"]:
        records = read_json_file(path, cfg, logger)
        if records:
            json_records.extend(records)
    log_stage(
        logger, "EXTRACT",
        f"Successfully read {len(combined_csv):,} raw rows from CSV" 
        f"and {len(json_records):,} objects from JSON."
    )
    return combined_csv, json_records

# ------ Transform  ------
# handle the "two hundred" / "50" mixed quantity column
_WORD_TO_NUM = {
    "zero": 0,
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5,
    "six": 6,
    "seven": 7,
    "eight": 8,
    "nine": 9,
    "ten": 10,
    "hundred": 100,
}

def _parse_quantity(value) -> float | None:
    if pd.isna(value):
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = str(value).strip().lower()
    if text.isdigit():
        return float(text)

    words = text.split()
    if not words:
        return None

    total = 0
    current = 0
    for w in words:
        if w not in _WORD_TO_NUM:
            return None  # unrecognized quantity phrase
        n = _WORD_TO_NUM[w]
        if n == 100:
            current = max(current, 1) * 100
        else:
            current += n
    total += current
    return float(total)


# handle missing IDs
def handle_missing_ids(df: pd.DataFrame, id_column: str, logger: logging.Logger) -> pd.DataFrame:
    """Splits rows to clean or quarantine based on missing IDs."""
    missing_mask = df[id_column].isna()
    quarantined = df[missing_mask].copy()
    clean = df[~missing_mask].copy()

    if len(quarantined):
        log_stage(logger, "TRANSFORM", f"Quarantined {len(quarantined):,} rows with missing '{id_column}'.", level="warning")

    return clean, quarantined

# standardize date format to ISO standards
def standardize_dates(df: pd.DataFrame, date_column: str, logger: logging.Logger) -> pd.DataFrame:
    """Standardize date formats to ISO YYYY-MM-DD."""
    def parse_one(value):
        if pd.isna(value):
            return None
        for fmt in ("%d/%m/%Y", "%Y-%m-%d", "%m/%d/%Y", "%d-%b-%Y"):
            try:
                return datetime.strptime(value, fmt).strftime("%Y-%m-%d")
            except (ValueError, TypeError):
                continue
        return None

    df = df.copy()
    before_valid = df[date_column].notna().sum()
    df[date_column] = df[date_column].apply(parse_one)
    after_valid = df[date_column].notna().sum()

    log_stage(
        logger, "TRANSFORM",
        f"Standardized '{date_column}' to ISO format."
        f"Valid dates before: {before_valid:,}, after: {after_valid:,}."
    )
    return df

# drop exact duplicate rows
def drop_duplicate_rows(df: pd.DataFrame, subset: list[str] | None, logger: logging.Logger) -> pd.DataFrame:
    """Drops duplicates."""
    before = len(df)
    df = df.drop_duplicates(subset=subset)
    dropped = before - len(df)

    log_stage(logger, "TRANSFORM", f"Dropped {dropped:,} duplicate rows" + (f" (subset={subset})" if subset else "") + ".")

    return df, dropped

# cast data types
def cast_data_types(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Casts Quantity (word-numbers -> int) and Price to proper numeric types."""
    df = df.copy()
 
    df["Quantity"] = df["Quantity"].apply(_parse_quantity)
    unparseable_qty = df["Quantity"].isna().sum()

    df["Price"] = pd.to_numeric(df["Price"], errors="coerce")
    missing_price = df["Price"].isna().sum()

    log_stage(
        logger, "TRANSFORM",
        f"Cast types: {unparseable_qty:,} unparseable Quantity value(s), "
        f"{missing_price:,} missing Price value(s) coerced to null (impute or drop downstream)."
    )
    return df


# reshape tabular data into relational structure for sqlite and nested docs for mongodb
def reshape_data(cfg: PipelineConfig, raw_df: pd.DataFrame, logger: logging.Logger):
    clean, quarantined = handle_missing_ids(raw_df, id_column="Product ID", logger=logger)
    clean = standardize_dates(clean, date_column="Last Restocked", logger=logger)
    clean, dropped_count = drop_duplicate_rows(clean, subset=["Product ID", "Warehouse"], logger=logger)
    clean = cast_data_types(clean, logger=logger)

    def parse_date(value):
        if pd.isna(value):
            return None
        try:
            return datetime.strptime(str(value).strip(), "%d-%m-%Y").strftime("%Y-%m-%d")
        except ValueError:
            return None
    clean["Last Restocked"] = clean["Last Restocked"].apply(parse_date)

    before = len(clean)
    clean = clean.drop_duplicates(subset=["Product ID", "Warehouse"])
    dropped_count = before - len(clean)

    clean["Product Name"] = clean["Product Name"].astype(str).str.strip().str.lower()
    clean["Quantity"] = clean["Quantity"].apply(_parse_quantity)
    clean["Price"] = pd.to_numeric(clean["Price"], errors="coerce")

    fact_inventory = clean.rename(columns={ # one consistent name

        "Product ID" : "Product_id", "Product Name" : "product_name",
        "Category" : "category", "Warehouse" : "warehouse_id",
        "Location" : "location", "Quantity" : "quantity", "Price" : "price",
        "Supplier" : "supplier", "Status" : "status", "Last Restocked" : "last_restocked",
    })

    dim_warehouse = pd.DataFrame({"warehouse_id": fact_inventory["warehouse_id"].unique()})
    dim_warehouse.insert(0, "warehouse_key", range(1, len(dim_warehouse) + 1))

    documents = [
        {
            "product_id": row["product_ID"],
            "product_name": row["product_name"],
            "category": row["category"],
            "warehouse": {
                "warehouse_id": row["warehouse_id"],
                "location": row["location"],
                "quantity": row["quantity"],
                "status": row["status"],
            },
            "price": row["price"],
            "date": row["last_restocked"],
            "last_restocked": row["last_restocked"],
        }
        for _, row in fact_inventory.iterrows()
    ]


    log_stage(
        logger, "TRANSFORM",
        f"Dropped {dropped_count:,} duplicate rows. Standardized dates and "
        f"imputed missing customer values. Reshaped structures."
    )

    return fact_inventory, dim_warehouse, documents, quarantined




# ------ Load ------
# insert structured rows into sqlite tables
def load_sqlite(cfg: PipelineConfig, fact_df: pd.DataFrame, dim_df: pd.DataFrame, logger: logging.Logger) -> int:
    output_dir = Path(cfg.SQLITE_OUTPUT_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_file = output_dir / "warehouse.db"

    conn = sqlite3.connect(db_file)
    try:
        dim_df.to_sql("dim_warehouse", conn, if_exists="replace", index=False)
        fact_df.to_sql(cfg.SQLITE_OUTPUT_TABLE, conn, if_exists="replace", index=False)
        conn.commit()

        sqlite_row_count = conn.execute(
            f"SELECT COUNT(*) FROM {cfg.SQLITE_OUTPUT_TABLE}"
        ).fetchone()[0]

    finally:
        conn.close()


    log_stage(
        logger, "LOAD - SQLITE", 
        f"Successfully inserted {sqlite_row_count:,} rows"
        f"into SQLite table '{cfg.SQLITE_OUTPUT_TABLE}'."
    )


# push nested docs into mongodb collections
def load_mongodb(cfg: PipelineConfig, documents: list[dict], logger: logging.Logger) -> int:
    logger = setup_logger(cfg.LOG_PATH)

    client = pymongo.MongoClient(cfg.MONGO_URI)
    try:
        db = client[cfg.MONGO_DB_NAME]
        collection = db[cfg.MONGO_COLLECTION_NAME]

        collection.delete_many({})  # clear existing documents
        
        if documents:
            collection.insert_many(documents)

        mongo_doc_count = collection.count_documents({})
    finally:
        client.close()

    log_stage(logger, "LOAD - MONGODB", f"Successfully inserted {mongo_doc_count:,} documents into collection '{cfg.MONGO_DB_NAME}.{cfg.MONGO_COLLECTION_NAME}'.")

    return mongo_doc_count

# ------ Execution ------
# execute all
def run_pipeline() -> dict:
    start_time = time.perf_counter()
    cfg = get_config()
    logger = setup_logger(cfg.LOG_PATH)

    raw_csv , json_records = fetch_source_batches(cfg, logger)
    fact_df, dim_df, documents, quarantined = reshape_data(cfg, raw_csv, logger)

    if len(quarantined):
        quarantine_dir = Path(cfg.QUARANTINE_FILES_PATH)
        quarantine_dir.mkdir(parents=True, exist_ok=True)
        quarantined.to_csv(quarantine_dir / "quarantined_rows.csv", index=False)

    sqlite_row_count = load_sqlite(cfg, fact_df, dim_df, logger)
    mongo_doc_count = load_mongodb(cfg, documents, logger)

    elapsed = time.perf_counter() - start_time
    log_stage(logger, "COMPLETE", f"Multi-target pipeline execution finished successfully in {elapsed:.2f} seconds.")

    return {
        "raw_rows": len(raw_csv),
        "json_objects": len(json_records),
        "quarantined_rows": len(quarantined),
        "sqlite_rows_loaded": sqlite_row_count,
        "mongo_docs_loaded": mongo_doc_count,
        "elapsed_seconds": round(elapsed, 2),
    }

if __name__ == "__main__":
    summary = run_pipeline()
    print(summary)


    