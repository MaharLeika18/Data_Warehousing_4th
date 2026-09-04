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
    return PipelineConfig(
        LANDING_ZONE_PATH = "lesson_4/landing_zone",
        CSV_GLOB_PATTERN = "*.csv",
        JSON_GLOB_PATTERN = "*.json",
        PROCESSED_PATH = "lesson_4/landing_zone/_processed",
        QUARANTINE_FILES_PATH = "lesson_4/quarantine",
        SQLITE_OUTPUT_PATH = "lesson_4/data/SQLite",
        SQLITE_OUTPUT_TABLE = "fact_sales",
        MONGO_URI = "mongodb+srv://louezethesaldua_db_user:c755vmW2I5vIjfOi@lesson4.ewe9gss.mongodb.net",
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
def fetch_source_batches(cfg: PipelineConfig, ):
    logger = setup_logger(cfg.LOG_PATH)
    ...     # this means insert function logic here
    log_stage(logger, "EXTRACT", f"Successfully read {len(combined_csv):,} raw rows from CSV and {len(json_records):,} objects from JSON.")


# ------ Transform  ------
# handle missing IDs


# standardize date format to ISO standards


# drop exact duplicate rows


# cast data types


# reshape tabular data into relational structure for sqlite and nested docs for mongodb
def reshape_data(cfg: PipelineConfig, ):
    logger = setup_logger(cfg.LOG_PATH)
    ...
    log_stage(logger, "TRANSFORM", f"Dropped {dropped_count:,} duplicate rows. Standardized dates and imputed missing customer values. Reshaped structures.")


# ------ Load ------
# insert structured rows into sqlite tables 
def load_sqlite(cfg: PipelineConfig, ):
    logger = setup_logger(cfg.LOG_PATH)
    ...
    log_stage(logger, "LOAD - SQLITE", f"Successfully inserted {sqlite_row_count:,} rows into SQLite table '{cfg.SQLITE_OUTPUT_TABLE}'.")


# push nested docs into mongodb collections
def load_mongodb(cfg: PipelineConfig, ):
    logger = setup_logger(cfg.LOG_PATH)
    ...
    log_stage(logger, "LOAD - MONGODB", f"Successfully inserted {mongo_doc_count:,} documents into collection '{cfg.MONGO_DB_NAME}.{cfg.MONGO_COLLECTION_NAME}'.")


# ------ Execution ------
# execute all
def run_pipeline() -> dict:
    start_time = time.perf_counter()
    cfg = get_config()
    logger = setup_logger(cfg.LOG_PATH)
    ...
    elapsed = time.perf_counter() - start_time
    log_stage(logger, "COMPLETE", f"Multi-target pipeline execution finished successfully in {elapsed:.2f} seconds.")