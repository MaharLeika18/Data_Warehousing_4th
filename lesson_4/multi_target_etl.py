from __future__ import annotations
import pandas as pd
import numpy as np
import os
import json
import sqlite3
import pymongo
from pymongo import MongoClient
from pathlib import Path
import logging
from pathlib import Path
from dataclasses import dataclass
from datetime import datetime, timezone
import time
from dotenv import load_dotenv
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError

@dataclass
class PipelineConfig:
    LANDING_ZONE_PATH: str
    CSV_GLOB_PATTERN: str
    JSON_GLOB_PATTERN:str
    PROCESSED_PATH: str
    SQLITE_OUTPUT_PATH: str
    SQLITE_OUTPUT_TABLE: str
    SQLITE_UNIQUE_KEY_COLUMNS: list
    MONGO_URI: str
    MONGO_DB_NAME: str
    MONGO_COLLECTION_NAME: str
    MONGO_PROOF_OUTPUT_PATH: str
    MONGO_UNIQUE_KEY_FIELDS: list
    LOG_PATH: str
    DATETIME_FORMATS: dict

def get_config() -> PipelineConfig:
    load_dotenv()
    mongo_uri = os.environ.get("MONGODB_URI")
    if not mongo_uri:
        raise RuntimeError(
            "MONGO_URI is not set. Copy .env.example to .env and set your MongoDB connection string."
        )
    
    return PipelineConfig(
        LANDING_ZONE_PATH = "lesson_4/landing_zone",
        CSV_GLOB_PATTERN = "*.csv",
        JSON_GLOB_PATTERN = "*.json",
        PROCESSED_PATH = "lesson_4/landing_zone/_processed",
        SQLITE_OUTPUT_PATH = "lesson_4/data/SQLite",
        SQLITE_OUTPUT_TABLE = "fact_sales",
        SQLITE_UNIQUE_KEY_COLUMNS = ["product_id"],
        MONGO_URI = mongo_uri,
        MONGO_DB_NAME = "warehouse_db",
        MONGO_COLLECTION_NAME = "sales_documents",
        MONGO_PROOF_OUTPUT_PATH = "lesson_4/data/MongoDB/mongodb_verification.json",
        MONGO_UNIQUE_KEY_FIELDS = ["product_id", "_source_file", "_source_line"],
        LOG_PATH= "lesson_4/logs",
        DATETIME_FORMATS = {
            "last_restocked": "%d/%m/%Y",
        }
    )

# ------ Logging ------
def setup_logger(log_path: str, level: str = "info") -> logging.Logger:
    log_dir = Path(log_path)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("pipeline")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    file_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")    
    file_handler = logging.FileHandler(log_dir / "pipeline_execution.log")
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
    file_path: Path, logger: logging.Logger
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

def ensure_sqlite_table(cfg: PipelineConfig, logger: logging.Logger) -> None:
    db_file = Path(cfg.SQLITE_OUTPUT_PATH) / "retail_warehouse.db"
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    try:
        cursor = conn.cursor()

        # Match the table to the columns produced by reshape_data.
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {cfg.SQLITE_OUTPUT_TABLE} (
                product_id TEXT,
                product_name TEXT,
                category TEXT,
                warehouse_id TEXT,
                location TEXT,
                quantity INTEGER,
                price REAL,
                supplier TEXT,
                status TEXT,
                last_restocked TEXT,
                _source_file TEXT,
                UNIQUE({", ".join(cfg.SQLITE_UNIQUE_KEY_COLUMNS)})
            )
        """)

        required_columns = {
            "product_id": "TEXT",
            "product_name": "TEXT",
            "category": "TEXT",
            "warehouse_id": "TEXT",
            "location": "TEXT",
            "quantity": "INTEGER",
            "price": "REAL",
            "supplier": "TEXT",
            "status": "TEXT",
            "last_restocked": "TEXT",
            "_source_file": "TEXT",
        }
        existing_columns = {
            row[1]
            for row in cursor.execute(
                f"PRAGMA table_info({cfg.SQLITE_OUTPUT_TABLE})"
            )
        }
        for column, data_type in required_columns.items():
            if column not in existing_columns:
                cursor.execute(
                    f"ALTER TABLE {cfg.SQLITE_OUTPUT_TABLE} "
                    f"ADD COLUMN {column} {data_type}"
                )
        conn.commit()
    finally:
        conn.close()

def ensure_mongo_index(cfg: PipelineConfig, logger: logging.Logger) -> None:
    client = MongoClient(cfg.MONGO_URI)
    try:
        collection = client[cfg.MONGO_DB_NAME][cfg.MONGO_COLLECTION_NAME]
        index_spec = [(field, 1) for field in cfg.MONGO_UNIQUE_KEY_FIELDS]
        collection.create_index(index_spec, unique=True, name="uniq_doc_key")
    finally:
        client.close()

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
        records = read_json_file(path, logger)
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
def handle_missing_ids(df: pd.DataFrame, id_column: str, logger: logging.Logger) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Splits rows to clean or quarantine based on missing IDs."""
    missing_mask = df[id_column].isna()
    quarantined = df[missing_mask].copy()
    clean = df[~missing_mask].copy()

    if len(quarantined):
        log_stage(logger, "TRANSFORM", f"Quarantined {len(quarantined):,} rows with missing '{id_column}'.", level="warning")

    return clean, quarantined

# standardize date format to ISO standards
def standardize_dates(
    df: pd.DataFrame, date_column: str, cfg: PipelineConfig, logger: logging.Logger
) -> pd.DataFrame:
    fmt = cfg.DATETIME_FORMATS.get(date_column)
    if not fmt:
        logger.warning(
            f"[TRANSFORM] No format configured for '{date_column}' in "
            f"DATETIME_FORMATS — skipping standardization."
        )
        return df

    def parse_one(value):
        if pd.isna(value):
            return None
        try:
            return datetime.strptime(str(value).strip(), fmt).strftime("%Y-%m-%d")
        except (ValueError, TypeError):
            return None

    df = df.copy()
    before_valid = df[date_column].notna().sum()
    df[date_column] = df[date_column].apply(parse_one)
    after_valid = df[date_column].notna().sum()

    newly_invalid = before_valid - after_valid

    log_stage(
        logger, "TRANSFORM",
        f"Standardized '{date_column}' to ISO format using pattern '{fmt}'. "
        f"Valid dates before: {before_valid:,}, after: {after_valid:,}."
    )

    if newly_invalid > 0:
        logger.warning(
            f"[TRANSFORM] {newly_invalid:,} value(s) in '{date_column}' "
            f"did not match format '{fmt}' and were set to None."
        )

    return df

# drop exact duplicate rows
def drop_duplicate_rows(df: pd.DataFrame, subset: list[str] | None, logger: logging.Logger) -> tuple[pd.DataFrame, int]:
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


def drop_invalid_inventory_rows(df: pd.DataFrame, logger: logging.Logger) -> pd.DataFrame:
    """Drop rows missing or containing negative required inventory values."""
    required = ["Product ID", "Quantity", "Price", "Last Restocked"]
    invalid = df[required].isna().any(axis=1)
    invalid |= pd.to_numeric(df["Product ID"], errors="coerce").lt(0).fillna(False)
    invalid |= pd.to_numeric(df["Quantity"], errors="coerce").lt(0).fillna(False)
    invalid |= pd.to_numeric(df["Price"], errors="coerce").lt(0).fillna(False)

    dropped = int(invalid.sum())
    if dropped:
        log_stage(
            logger,
            "TRANSFORM",
            f"Dropped {dropped:,} row(s) with missing or negative values in "
            "product_id, quantity, price, or last_restocked.",
            level="warning",
        )
    return df.loc[~invalid].copy()


# reshape tabular data into relational structure for sqlite and nested docs for mongodb
def reshape_data(cfg: PipelineConfig, raw_df: pd.DataFrame, logger: logging.Logger):
    clean, quarantined = handle_missing_ids(raw_df, id_column="Product ID", logger=logger)
    clean = standardize_dates(clean, date_column="Last Restocked", cfg=cfg, logger=logger)
    clean, dropped_count = drop_duplicate_rows(clean, subset=["Product ID", "Warehouse"], logger=logger)
    clean = cast_data_types(clean, logger=logger)
    clean = drop_invalid_inventory_rows(clean, logger=logger)

    clean["Product Name"] = clean["Product Name"].astype(str).str.strip().str.lower()

    fact_inventory = clean.rename(columns={
        "Product ID": "product_id", "Product Name": "product_name",
        "Category": "category", "Warehouse": "warehouse_id",
        "Location": "location", "Quantity": "quantity", "Price": "price",
        "Supplier": "supplier", "Status": "status", "Last Restocked": "last_restocked",
    })

    dim_warehouse = pd.DataFrame({"warehouse_id": fact_inventory["warehouse_id"].unique()})
    dim_warehouse.insert(0, "warehouse_key", range(1, len(dim_warehouse) + 1))

    documents = [
        {
            "product_id": row["product_id"],
            "product_name": row["product_name"],
            "category": row["category"],
            "warehouse": {
                "warehouse_id": row["warehouse_id"],
                "location": row["location"],
                "quantity": row["quantity"],
                "status": row["status"],
            },
            "price": row["price"],
            "last_restocked": row["last_restocked"],
        }
        for _, row in fact_inventory.iterrows()
    ]

    log_stage(
        logger, "TRANSFORM",
        f"Dropped {dropped_count:,} duplicate rows. Standardized dates and "
        f"cast numeric types. Reshaped structures."
    )

    return fact_inventory, dim_warehouse, documents, quarantined

# ------ Verify ------
def verify_mongo_insertion(cfg: PipelineConfig, logger: logging.Logger, expected_count: int | None = None) -> dict:
    client = MongoClient(cfg.MONGO_URI)
    try:
        db = client[cfg.MONGO_DB_NAME]
        collection = db[cfg.MONGO_COLLECTION_NAME]

        actual_count = collection.count_documents({})
        samples = list(collection.find().limit(5))

        field_types: dict[str, set] = {}
        for doc in samples:
            for key, value in doc.items():
                field_types.setdefault(key, set()).add(type(value).__name__)

        field_type_summary = {k: sorted(v) for k, v in field_types.items()}

        def _serialize(value):
            """Recursively convert Mongo values to strict-JSON-safe values."""
            if isinstance(value, dict):
                return {key: _serialize(item) for key, item in value.items()}
            if isinstance(value, (list, tuple)):
                return [_serialize(item) for item in value]
            if isinstance(value, (float, np.floating)) and not np.isfinite(value):
                return None
            if isinstance(value, np.generic):
                return value.item()
            if value.__class__.__name__ == "ObjectId":
                return str(value)
            return value

        report = {
            "verified_at_utc": datetime.now(timezone.utc).isoformat(),
            "database": cfg.MONGO_DB_NAME,
            "collection": cfg.MONGO_COLLECTION_NAME,
            "document_count": actual_count,
            "expected_count": expected_count,
            "count_matches_expected": (
                actual_count == expected_count if expected_count is not None else None
            ),
            "field_type_summary": field_type_summary,
            "sample_documents": [_serialize(d) for d in samples],
        }

        output_path = Path(cfg.MONGO_PROOF_OUTPUT_PATH)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(json.dumps(report, indent=2, default=str, allow_nan=False))

        log_stage(
            logger, "LOAD - MONGODB",
            f"Verified {actual_count:,} documents in collection "
            f"'{cfg.MONGO_DB_NAME}.{cfg.MONGO_COLLECTION_NAME}'. "
            f"Proof written to {output_path}."
        )

        if expected_count is not None and actual_count != expected_count:
            logger.warning(
                f"[LOAD - MONGODB] Count mismatch: expected {expected_count:,}, "
                f"found {actual_count:,} in collection."
            )

        return report

    finally:
        client.close()


# ------ Load ------
# insert structured rows into sqlite tables
def load_sqlite(cfg: PipelineConfig, fact_df: pd.DataFrame, dim_df: pd.DataFrame, logger: logging.Logger) -> None:
    output_dir = Path(cfg.SQLITE_OUTPUT_PATH)
    output_dir.mkdir(parents=True, exist_ok=True)
    db_file = output_dir / "retail_warehouse.db"

    ensure_sqlite_table(cfg, logger)
    conn = sqlite3.connect(db_file)
    try:
        dim_df.to_sql("dim_warehouse", conn, if_exists="replace", index=False)

        columns = list(fact_df.columns)
        column_sql = ", ".join(columns)
        placeholders = ", ".join("?" for _ in columns)
        rows = fact_df.where(pd.notna(fact_df), None).itertuples(index=False, name=None)
        conn.executemany(
            f"INSERT OR IGNORE INTO {cfg.SQLITE_OUTPUT_TABLE} "
            f"({column_sql}) VALUES ({placeholders})",
            rows,
        )
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
    ensure_mongo_index(cfg, logger)

    client = pymongo.MongoClient(cfg.MONGO_URI)
    try:
        db = client[cfg.MONGO_DB_NAME]
        collection = db[cfg.MONGO_COLLECTION_NAME]

        collection.delete_many({})  # clear existing documents
        
        if documents:
            operations = [
                ReplaceOne(
                    {field: document.get(field) for field in cfg.MONGO_UNIQUE_KEY_FIELDS},
                    document,
                    upsert=True,
                )
                for document in documents
            ]
            collection.bulk_write(operations, ordered=False)

        mongo_doc_count = collection.count_documents({})
    finally:
        client.close()

    log_stage(logger, "LOAD - MONGODB", f"Successfully inserted {mongo_doc_count:,} documents into collection '{cfg.MONGO_DB_NAME}.{cfg.MONGO_COLLECTION_NAME}'.")
    verify_mongo_insertion(cfg, logger, expected_count=mongo_doc_count)

    return mongo_doc_count

# ------ Execution ------
# execute all
def run_pipeline() -> dict:
    start_time = time.perf_counter()
    cfg = get_config()
    logger = setup_logger(cfg.LOG_PATH)

    raw_csv , json_records = fetch_source_batches(cfg, logger)
    fact_df, dim_df, documents, quarantined = reshape_data(cfg, raw_csv, logger)

    sqlite_row_count = load_sqlite(cfg, fact_df, dim_df, logger)
    mongo_doc_count = load_mongodb(cfg, documents, logger)

    elapsed = time.perf_counter() - start_time
    log_stage(logger, "COMPLETE", f"Multi-target pipeline execution finished successfully in {elapsed:.2f} seconds.")

    return {
        "raw_rows": len(raw_csv),
        "json_objects": len(json_records),
        "sqlite_rows_loaded": sqlite_row_count,
        "mongo_docs_loaded": mongo_doc_count,
        "elapsed_seconds": round(elapsed, 2),
    }

if __name__ == "__main__":
    summary = run_pipeline()
    print(summary)


    