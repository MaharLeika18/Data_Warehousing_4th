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
from datetime import datetime, timezone
import time
from dotenv import load_dotenv
from pymongo import ReplaceOne
from pymongo.errors import BulkWriteError
from dataclasses import dataclass, field
from typing import Literal

@dataclass
class TableConfig:
    source_file: str          
    destination: Literal["sqlite", "mongo"]
    output_name: str           
    unique_key_columns: list[str]

@dataclass
class PipelineConfig:
    LANDING_ZONE_PATH: str
    PROCESSED_PATH: str
    SQLITE_OUTPUT_PATH: str
    MONGO_URI: str
    MONGO_DB_NAME: str
    MONGO_PROOF_OUTPUT_PATH: str
    LOG_PATH: str
    DATETIME_FORMATS: dict
    TABLES: list[TableConfig] = field(default_factory=list)

def get_config():
    load_dotenv()
    mongo_uri = os.environ.get("MONGODB_URI")
    if not mongo_uri:
        raise RuntimeError(
            "MONGO_URI is not set. Copy .env.example to .env and set your MongoDB connection string."
        )

    return PipelineConfig(
        LANDING_ZONE_PATH="ladringan_saldua_midterms/landing_zone/archive",
        PROCESSED_PATH="ladringan_saldua_midterms/landing_zone/_processed",
        SQLITE_OUTPUT_PATH="ladringan_saldua_midterms/data/SQLite",
        MONGO_URI=mongo_uri,
        MONGO_DB_NAME="VLE_Logs",
        MONGO_PROOF_OUTPUT_PATH="ladringan_saldua_midterms/data/MongoDB/mongodb_verification.json",
        LOG_PATH="ladringan_saldua_midterms/logs",
        DATETIME_FORMATS={
            "last_restocked": "%d/%m/%Y",
            "enrollment_date": "%Y-%m-%d",
        },
        TABLES=[
            TableConfig("courses.csv", "sqlite", "dim_courses", ["code_module", "code_presentation"]),
            TableConfig("assessments.csv",  "sqlite", "dim_assessments", ["id_assessment"]),
            TableConfig("vle.csv", "sqlite", "dim_vles", ["id_site"]),
            TableConfig("studentInfo.csv", "sqlite", "dim_students", ["code_module", "code_presentation", "id_student"]),
            TableConfig("studentRegistration.csv", "sqlite", "fact_registration", ["code_module", "code_presentation", "id_student"]),
            TableConfig("studentAssessment.csv", "sqlite", "fact_assessment_results", ["id_assessment", "id_student"]),
            TableConfig("studentVle.csv", "mongo", "clickstream_events", ["code_module", "code_presentation", "id_student", "id_site", "date"]),
        ],
    )

# ------ Logging ------
def setup_logger(log_path: str, level: str = "info") -> logging.Logger:
    log_dir = Path(log_path)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("pipeline")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    file_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")    
    file_handler = logging.FileHandler(log_dir / "milestone1_log.txt")
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
def load_csv_to_sqlite(path: str, table_name: str, sqlite_db_path: str, logger: logging.Logger):
    stage = "CSV to SQLite"
    source_file = os.path.basename(path)

    log_stage(logger, stage, f"Starting load of {source_file} into '{table_name}'")

    try:
        df = pd.read_csv(path)
        log_stage(logger, stage, f"Read {len(df)} rows, {len(df.columns)} columns from {source_file}")
    except FileNotFoundError:
        log_stage(logger, stage, f"File not found: {path}", level="error")
        raise
    except Exception as e:
        log_stage(logger, stage, f"Failed to read {source_file}: {e}", level="error")
        raise

    try:
        os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
        conn = sqlite3.connect(sqlite_db_path)
        df.to_sql(table_name, conn, if_exists="replace", index=False)
        conn.close()
    except Exception as e:
        log_stage(logger, stage, f"Failed to write to SQLite: {e}", level="error")
        raise

    log_stage(logger, stage, f"Finished: wrote {len(df)} rows to '{table_name}' in {sqlite_db_path}")

def load_clickstream_to_mongo(
    path: str, db_name: str, collection_name: str, mongo_uri: str, unique_key_columns: list[str], logger: logging.Logger, chunk_size: int = 50_000,
):
    stage = "clickstream to MongoDB"
    source_file = os.path.basename(path)

    log_stage(logger, stage, f"Starting load of {source_file} into '{collection_name}'")

    client = MongoClient(mongo_uri)
    collection = client[db_name][collection_name]

    # Enforce no duplicate events based on unique key columns
    collection.create_index(
        [(col, 1) for col in unique_key_columns],
        unique=True,
        name="uniq_studentvle_event",
    )
    log_stage(logger, stage, f"Ensured unique index on {unique_key_columns}")

    source_file = os.path.basename(path)
    total_inserted = 0
    total_skipped = 0

    try: 
        for chunk_idx, chunk in enumerate(pd.read_csv(path, chunksize=chunk_size)):
            docs = chunk.to_dict(orient="records")

            for i, doc in enumerate(docs):
                doc["_source_file"] = source_file
                doc["_source_line"] = chunk_idx * chunk_size + i

            try:
                result = collection.insert_many(docs, ordered=False)    # lets valid docs insert even if some are duplicates
                inserted = len(result.inserted_ids)
                total_inserted += len(result.inserted_ids)
                log_stage(logger, stage, f"Chunk {chunk_idx}: inserted {inserted}/{len(docs)} rows")
            except BulkWriteError as bwe:
                inserted = bwe.details.get("nInserted", 0)
                skipped = len(docs) - inserted
                total_inserted += inserted
                total_skipped += len(docs) - inserted
                log_stage(
                    logger, stage,
                    f"Chunk {chunk_idx}: inserted {inserted}, skipped {skipped} duplicates",
                    level="warning",
                )
    except FileNotFoundError:
        log_stage(logger, stage, f"File not found: {path}", level="error")
        raise
    except Exception as e:
        log_stage(logger, stage, f"Unexpected error: {e}", level="error")
        raise
    finally:
        client.close()

    log_stage(
        logger, stage,
        f"Finished: {total_inserted} inserted, {total_skipped} skipped from {source_file}",
    )    
    print(f"Done: {total_inserted} inserted, {total_skipped} skipped (duplicates) from {source_file}")

# ------ Transform  ------


# ------ Verify ------


# ------ Load ------


# ------ Execution ------
def run_pipeline() -> dict:
    start_time = time.perf_counter()
    cfg = get_config()
    logger = setup_logger(cfg.LOG_PATH)
    log_stage(logger, "pipeline", "Pipeline run started")

    for table in cfg.TABLES:
        path = os.path.join(cfg.LANDING_ZONE_PATH, table.source_file)

        if table.destination == "sqlite":
            log_stage(logger, table.output_name, f"Loading {table.source_file} into SQLite")
            load_csv_to_sqlite(
                path, table.output_name,
                os.path.join(cfg.SQLITE_OUTPUT_PATH, "warehouse.db"),
                logger,
            )
        elif table.destination == "mongo":
            load_clickstream_to_mongo(
                path, cfg.MONGO_DB_NAME, table.output_name,
                cfg.MONGO_URI, table.unique_key_columns, logger,
            )

    ...

    elapsed = time.perf_counter() - start_time
    log_stage(logger, "COMPLETE", f"Multi-target pipeline execution finished successfully in {elapsed:.2f} seconds.")
    
    return {
        "elapsed_seconds": round(elapsed, 2),
    }

if __name__ == "__main__":
    summary = run_pipeline()
    print(summary)
