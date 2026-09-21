from __future__ import annotations
from venv import logger
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

def transform_student_vle(df: pd.DataFrame, table: TableConfig, quarantine: Quarantine, logger: logging.logger, parents:dict[str, pd.DataFrame], quiet: bool = False) -> pd.DataFrame:
    stage, name, n_in = "Transform", table.output_name, len(df)
    df = df.copy()
    df.columns = [c.strip().lower for c in df.columns]
    df = df[[c for c in df.columns if c in table.columns or c.startswitch("_")]]

    for col in table.columns:
        s = df[col].astype("string").str.strip()
        df[col] = s.mask(s.str.lower().isin(NULL_TOKENS))

    bad = pd.Series(False, index=df.index)
    for col in table.required_ints:
        num = pd.to_numeric(df[col], errors="coerce").astype("float64")
        was_null = df[col].isna().to_numpy()
        fractional = (num.notna() & (num % 1 != 0)).to_numpy()
        quarantine.add(df[was_null & ~bad.to_numpy()], name, f"null {col}")
        quarantine.add(df[num.isna().to_numpy() & ~was_null & ~bad.to_numpy()], name, f"non-numeric {col}")
        quarantine.add(df[fractional & ~bad.to_numpy()], name, f"non-integer {col}")
        bad |= num.isna() | fractional
        df[col] = num
    df = df[~bad].copy()
    for col in table.required_ints:
        df[col] = df[col].astype("Int64")


    for col in table.optional_ints + table.optional_floats:
        num = pd.to_numeric(df[col], errors="coerce").astype("float64")
        coerced = int((num.isna() & df[col].notna()).sum())
        if coerced and not quiet:
            log_stage(logger, stage, f"{name}.{col}: {coerced} unparseable values set to NULL", "warning")
        df[col] = num.round().astype("Int64") if col in table.optional_ints else num.astype("Float64")

    for col, style in table.text_case.items():
        df[col] = getattr(df[col].str, style)()

    for reason, fn in table.checks:
        mask = fn(df).fillna(False).astype(bool)
        quarantine.add(df[mask], name, reason)
        df = df[~mask]

    n_before = len(df)
    df = df.drop_duplicates(subset=table.columns)
    if n_before - len(df) and not quiet:
        log_stage(logger, stage, f"{name}: {n_before - len(df)} exact duplicate rows dropped")
    if table.destination == "sqlite":
        dup = df.duplicated(table.unique_key_columns, keep="first")
        quarantine.add(df[dup], name, f"duplicate key {table.unique_key_columns}")
        df = df[~dup]

    for cols, parent_name in table.references:
        parent = parents.get(parent_name)
        if parent is None:
            continue
        valid = set(zip(*(parent[c] for c in cols)))
        orphan = pd.Series([k not in valid for k in zip(*(df[c] for c in cols))], index=df.index, dtype=bool)
        quarantine.add(df[orphan], name, f"orphan: {cols} not in {parent_name}")
        df = df[~orphan]

    df = df.reset_index(drop=True)
    if not quiet:
        left = {c: int(v) for c, v in df[table.columns].isna().sum().items() if v}
        summary = f"{name}: in={n_in} out={len(df)} removed={n_in - len(df)}"
        log_stage(logger, stage, f"{summary} | remaining NULLs (nullable, kept): {left or 'none'}")
    return df

# ------ Verify ------


def verify_sqlite(sqlite_db_path: str, expected: dict[str, int], logger: logging.Logger) -> None:
    """Prove the SQLite warehouse is correct. Raises an error if any check fails.
        Check 1 - row counts : rows in each table == rows we loaded
        Check 2 - foreign keys: no child row points to a parent that does not exist
        Check 3 - integrity   : the database file is not corrupted
    """
    stage = "Verify SQLite"
    problems = []
    conn = sqlite3.connect(sqlite_db_path)
    try:
        for table, n_loaded in expected.items():                       # Check 1
            n_in_db = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            passed = n_in_db == n_loaded
            log_stage(logger, stage, f"[{'PASS' if passed else 'FAIL'}] row count {table:<26}" f"loaded={n_loaded}  in database={n_in_db}")
            if not passed:
                problems.append(f"{table} row count")

        broken = conn.execute("PRAGMA foreign_key_check").fetchall()   # Check 2
        log_stage(logger, stage, f"[{'PASS' if not broken else 'FAIL'}] foreign keys: {len(broken)} broken references")
        if broken:
            problems.append("foreign keys")

        integrity = conn.execute("PRAGMA integrity_check").fetchone()[0]   # Check 3
        log_stage(logger, stage, f"[{'PASS' if integrity == 'ok' else 'FAIL'}] integrity_check: {integrity}")
        if integrity != "ok":
            problems.append("integrity")
    finally:
        conn.close()

    if problems:
        raise RuntimeError(f"SQLite verification failed: {', '.join(problems)}")
    log_stage(logger, stage, "All SQLite checks PASSED")


def run_mongo_queries(coll, logger: logging.Logger) -> dict:
    """Two example analytics queries on the clickstream collection (results go in the proof file)."""
    stage = "Verify MongoDB"
    results = {}

    # Query 1: total clicks per activity type ($group + $sort)
    results["clicks_by_activity_type"] = list(coll.aggregate([
        {"$group": {"_id": "$activity_type", "clicks": {"$sum": "$sum_click"}, "events": {"$sum": 1}}},
        {"$sort": {"clicks": -1}}], allowDiskUse=True))
    for row in results["clicks_by_activity_type"][:6]:
        log_stage(logger, stage, f"  {str(row['_id']):<16} clicks={row['clicks']:<10} events={row['events']}")

    # Query 2: $facet = several small analyses computed in one pass over the data
    try:
        results["facet_summary"] = list(coll.aggregate([{"$facet": {
            "events_by_module": [{"$group": {"_id": "$code_module", "events": {"$sum": 1}}},{"$sort": {"_id": 1}}],
            "click_stats": [{"$group": {"_id": None, "total_clicks": {"$sum": "$sum_click"},
                                        "avg_clicks": {"$avg": "$sum_click"},
                                        "students": {"$addToSet": "$id_student"}}},
                            {"$project": {"_id": 0, "total_clicks": 1, "avg_clicks": 1,"distinct_students": {"$size": "$students"}}}]}}],
            allowDiskUse=True))
        log_stage(logger, stage, "$facet pipeline ran successfully")
    except Exception as e:
        log_stage(logger, stage, f"$facet pipeline skipped: {e}", "warning")
    return results


def verify_mongo(cfg: PipelineConfig, table: TableConfig, stats: dict, logger: logging.Logger) -> None:
    """Prove the MongoDB load worked, and save the evidence to mongodb_verification.json.
        Check 1 - document count in MongoDB == number of rows we inserted
        Check 2 - the unique index (duplicate protection) exists
    Raises an error if any check fails."""
    stage = "Verify MongoDB"
    client = MongoClient(cfg.MONGO_URI, serverSelectionTimeoutMS=5000)
    try:
        coll = client[cfg.MONGO_DB_NAME][table.output_name]
        in_db = coll.count_documents({})
        indexes = [ix["name"] for ix in coll.list_indexes()]
        proof = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "database": cfg.MONGO_DB_NAME,
            "collection": table.output_name,
            "load_stats": stats,                      # rows_read / rows_valid / inserted / skipped_duplicates
            "documents_in_mongodb": in_db,
            "indexes": indexes,
            "sample_documents": list(coll.find({}, {"_id": 0}).limit(3)),
        }
        proof.update(run_mongo_queries(coll, logger))
    finally:
        client.close()

    checks = {
        "document count matches inserted rows": in_db == stats["inserted"],
        "unique index exists": "uniq_studentvle_event" in indexes,
    }
    proof["checks"] = checks
    os.makedirs(os.path.dirname(cfg.MONGO_PROOF_OUTPUT_PATH), exist_ok=True)
    with open(cfg.MONGO_PROOF_OUTPUT_PATH, "w", encoding="utf-8") as fh:
        json.dump(proof, fh, indent=2, default=str)

    log_stage(logger, stage, f"Rows inserted by the loader = {stats['inserted']}, documents found in MongoDB = {in_db}")
    for check, passed in checks.items():
        log_stage(logger, stage, f"[{'PASS' if passed else 'FAIL'}] {check}")
    log_stage(logger, stage, f"Proof file written: {cfg.MONGO_PROOF_OUTPUT_PATH}")
    if not all(checks.values()):
        raise RuntimeError("MongoDB verification failed: " + ", ".join(k for k, v in checks.items() if not v))
    log_stage(logger, stage, "All MongoDB checks PASSED")


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

    step("Verify SQLite", lambda: verify_sqlite(sqlite_path, expected, logger))
    step("Window functions", lambda: run_window_functions(sqlite_path, logger))
    if mongo_stats:
        step("Verify MongoDB", lambda: verify_mongo(cfg, mongo_table, mongo_stats, logger))
    quarantine.flush()

    elapsed = time.perf_counter() - start_time
    log_stage(logger, "COMPLETE", f"Multi-target pipeline execution finished successfully in {elapsed:.2f} seconds.")
    
    return {
        "elapsed_seconds": round(elapsed, 2),
    }

if __name__ == "__main__":
    summary = run_pipeline()
    print(summary)
