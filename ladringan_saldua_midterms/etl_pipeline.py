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
from typing import Callable, Literal

# ------ Config  ------
@dataclass
class TableConfig:
    source_file: str
    destination: str
    output_name: str
    unique_key_columns: list[str]
    columns: list[str]                                  
    required_ints: list[str] = field(default_factory=list)
    optional_ints: list[str] = field(default_factory=list)
    optional_floats: list[str] = field(default_factory=list)
    text_case: dict[str, str] = field(default_factory=dict)     
    checks: list[tuple[str, Callable[..., bool]]] = field(default_factory=list) 
    references: list[tuple[list[str], str]] = field(default_factory=list)  

@dataclass
class PipelineConfig:
    LANDING_ZONE_PATH: str
    PROCESSED_PATH: str
    SQLITE_OUTPUT_PATH: str
    MONGO_URI: str
    MONGO_DB_NAME: str
    MONGO_PROOF_OUTPUT_PATH: str
    LOG_PATH: str
    QUARANTINE_PATH: str
    TABLES: list[TableConfig] = field(default_factory=list)

COLUMN_TYPES = {
    # required integers 
    "id_assessment": "required_int",
    "id_site": "required_int",
    "id_student": "required_int",
    "date": "required_int",              
    "date_submitted": "required_int",
    "is_banked": "required_int",
    "sum_click": "required_int",
    "num_of_prev_attempts": "required_int",
    "studied_credits": "required_int",
    "length": "required_int",

    # optional integers
    "week_from": "optional_int",
    "week_to": "optional_int",
    "date_registration": "optional_int",
    "date_unregistration": "optional_int",

    # optional floats
    "score": "optional_float",
    "weight": "optional_float",
}

NULL_TOKENS = {"", "na", "n/a", "null", "none", "nan", "-"}

def classify_columns(columns: list[str]) -> dict[str, list[str]]:
    required_ints, optional_ints, optional_floats = [], [], []
    for col in columns:
        kind = COLUMN_TYPES.get(col)
        if kind == "required_int":
            required_ints.append(col)
        elif kind == "optional_int":
            optional_ints.append(col)
        elif kind == "optional_float":
            optional_floats.append(col)
    return {
        "required_ints": required_ints,
        "optional_ints": optional_ints,
        "optional_floats": optional_floats,
    }

def make_table(source_file, destination, output_name, unique_key_columns, columns, text_case=None, checks=None, references=None):
    types = classify_columns(columns)
    return TableConfig(
        source_file=source_file,
        destination=destination,
        output_name=output_name,
        unique_key_columns=unique_key_columns,
        columns=columns,
        required_ints=types["required_ints"],
        optional_ints=types["optional_ints"],
        optional_floats=types["optional_floats"],
        text_case=text_case or {},
        checks=checks or [],
        references=references or [],
    )

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
        QUARANTINE_PATH="ladringan_saldua_midterms/quarantine",
        TABLES=[
            make_table(
                source_file="courses.csv",
                destination="sqlite",
                output_name="dim_courses",
                unique_key_columns=["code_module", "code_presentation"],
                columns=["code_module", "code_presentation", "module_presentation_length"],
                text_case={"code_module": "upper", "code_presentation": "upper"},
            ),
            make_table(
                source_file="assessments.csv",
                destination="sqlite",
                output_name="dim_assessments",
                unique_key_columns=["id_assessment"],
                columns=["code_module", "code_presentation", "id_assessment", "assessment_type", "date", "weight"],
                text_case={"code_module": "upper", "code_presentation": "upper"},
                references=[(["code_module", "code_presentation"], "courses")],
            ),
            make_table(
                source_file="vle.csv",
                destination="sqlite",
                output_name="dim_vles",
                unique_key_columns=["id_site"],
                columns=["id_site", "code_module", "code_presentation", "activity_type", "week_from", "week_to"],
                text_case={"code_module": "upper", "code_presentation": "upper"},
                references=[(["code_module", "code_presentation"], "courses")],
            ),
            make_table(
                source_file="studentInfo.csv",
                destination="sqlite",
                output_name="dim_students",
                unique_key_columns=["code_module", "code_presentation", "id_student"],
                columns=[
                    "code_module", "code_presentation", "id_student", "gender", "region",
                    "highest_education", "imd_band", "age_band", "num_of_prev_attempts",
                    "studied_credits", "disability", "final_result",
                ],
                text_case={"code_module": "upper", "code_presentation": "upper"},
                references=[(["code_module", "code_presentation"], "courses")],
            ),
            make_table(
                source_file="studentRegistration.csv",
                destination="sqlite",
                output_name="fact_registration",
                unique_key_columns=["code_module", "code_presentation", "id_student"],
                columns=["code_module", "code_presentation", "id_student", "date_registration", "date_unregistration"],
                text_case={"code_module": "upper", "code_presentation": "upper"},
                references=[
                    (["code_module", "code_presentation"], "courses"),
                    (["code_module", "code_presentation", "id_student"], "studentInfo"),
                ],
            ),
            make_table(
                source_file="studentAssessment.csv",
                destination="sqlite",
                output_name="fact_assessment_results",
                unique_key_columns=["id_assessment", "id_student"],
                columns=["id_assessment", "id_student", "date_submitted", "is_banked", "score"],
                checks=[("score out of range", lambda df: (df["score"] < 0) | (df["score"] > 100))],
                references=[(["id_assessment"], "assessments")],
            ),
            make_table(
                source_file="studentVle.csv",
                destination="mongo",
                output_name="clickstream_events",
                unique_key_columns=["code_module", "code_presentation", "id_student", "id_site", "date"],
                columns=["code_module", "code_presentation", "id_student", "id_site", "date", "sum_click"],
                text_case={"code_module": "upper", "code_presentation": "upper"},
                checks=[("negative sum_click", lambda df: df["sum_click"] < 0)],
                references=[
                    (["code_module", "code_presentation"], "courses"),
                    (["code_module", "code_presentation", "id_student"], "studentInfo"),
                    (["id_site"], "vle"),
                ],
            ),
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

# ------ Quarantine ------
def quarantine_bad_rows(
    df: pd.DataFrame, required_columns: list[str], source_file: str, quarantine_dir: str, logger: logging.Logger,
) -> pd.DataFrame:
    stage = f"{source_file} -> quarantine"
    mask_bad = df[required_columns].isna().any(axis=1)
    bad_rows = df[mask_bad]
    good_rows = df[~mask_bad]

    if len(bad_rows) > 0:
        os.makedirs(quarantine_dir, exist_ok=True)
        out_path = os.path.join(quarantine_dir, f"{source_file}_quarantined.csv")
        bad_rows.to_csv(out_path, index=False)
        log_stage(
            logger, stage,
            f"Quarantined {len(bad_rows)} of {len(df)} rows (missing one of {required_columns}) -> {out_path}",
            level="warning",
        )
    else:
        log_stage(logger, stage, f"No quarantined rows - all {len(df)} rows passed required-field check")

    return good_rows

class Quarantine:
    def __init__(self, quarantine_dir: str, logger: logging.Logger):
        self.quarantine_dir = quarantine_dir
        self.logger = logger

    def add(self, rows: pd.DataFrame, table_name: str, reason: str) -> None:
        if rows.empty:
            return
        os.makedirs(self.quarantine_dir, exist_ok=True)
        out_path = os.path.join(self.quarantine_dir, f"{table_name}.rejected.csv")
        rows = rows.copy()
        rows["_reject_reason"] = reason
        rows.to_csv(out_path, mode="a", header=not os.path.exists(out_path), index=False)
        log_stage(self.logger, "Quarantine", f"{table_name}: {len(rows)} rows quarantined ({reason})", "warning")

# ------ Extract ------
def read_csv_file(file_path: Path, logger: logging.Logger) -> pd.DataFrame | None:
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
        log_stage(logger, "Extract", f"Read {len(df)} rows, {len(df.columns)} columns from {file_path.name}")
        return df
    except UnicodeDecodeError as e:
        log_stage(logger, "Extract", f"Encoding error in {file_path.name}, retrying with latin-1: {e}", level="warning")
        try:
            df = pd.read_csv(file_path, encoding="latin-1", on_bad_lines="warn")
            df["_source_file"] = file_path.name
            return df
        except Exception as e2:
            log_stage(logger, "Extract", f"Failed to read {file_path.name} even with fallback encoding: {e2}", level="error")
            return None
    except (pd.errors.ParserError, pd.errors.EmptyDataError) as e:
        log_stage(logger, "Extract", f"Failed to parse {file_path.name}: {e}", level="error")
        return None
    
# ------ Transform  ------

def transform_table(
    df: pd.DataFrame, table: TableConfig, logger: logging.Logger,
    parents: dict[str, pd.DataFrame], quarantine: Quarantine, quiet: bool = False
) -> pd.DataFrame:
    stage, name, n_in = "Transform", table.output_name, len(df)
    log_stage(logger, stage, f"Starting transformation of '{name}' ({n_in} rows in)")

    df = df.copy()

    # column cleanup 
    df.columns = [c.strip().lower() for c in df.columns]
    df = df[[c for c in df.columns if c in table.columns or c.startswith("_")]]
    missing_columns = [col for col in table.columns if col not in df.columns]
    for col in missing_columns:
        df[col] = pd.NA
    if missing_columns:
        log_stage(logger, stage, f"{name}: missing configured columns filled with NULL: {missing_columns}", "warning")
    log_stage(logger, stage, f"{name}: normalized column names, kept {len(df.columns)} columns")

    # null token normalization 
    for col in table.columns:
        s = df[col].astype("string").str.strip()
        df[col] = s.mask(s.str.lower().isin(NULL_TOKENS))
    log_stage(logger, stage, f"{name}: normalized null-like tokens across {len(table.columns)} columns")

    # quarantine nulls in unique key columns 
    null = pd.Series(False, index=df.index)
    for col in table.unique_key_columns:
        null |= df[col].isna()
    if null.any():
        quarantine.add(df[null], name, f"null unique key {table.unique_key_columns}")
        log_stage(logger, stage, f"{name}: quarantined {int(null.sum())} rows with null unique key", "warning")
    df = df[~null].copy()

    # quarantine null/non-numeric/fractional in required integer columns
    bad = pd.Series(False, index=df.index)
    for col in table.required_ints:
        num = pd.to_numeric(df[col], errors="coerce").astype("float64")
        fractional = num.notna() & (num % 1 != 0)
        bad |= num.isna() | fractional
        df[col] = num

    if bad.any():
        quarantine.add(df[bad], name, f"invalid required int in {table.required_ints}")
        log_stage(logger, stage, f"{name}: quarantined {int(bad.sum())} rows with invalid required integer values", "warning")

    df = df[~bad].copy()
    for col in table.required_ints:
        df[col] = df[col].astype("Int64")
    log_stage(logger, stage, f"{name}: validated required integer columns {table.required_ints}")

    # coerce optional numeric columns 
    for col in table.optional_ints + table.optional_floats:
        num = pd.to_numeric(df[col], errors="coerce").astype("float64")
        coerced = int((num.isna() & df[col].notna()).sum())
        if coerced and not quiet:
            log_stage(logger, stage, f"{name}.{col}: {coerced} unparseable values set to NULL", "warning")
        df[col] = num.round().astype("Int64") if col in table.optional_ints else num.astype("Float64")
    if table.optional_ints or table.optional_floats:
        log_stage(logger, stage, f"{name}: coerced optional numeric columns {table.optional_ints + table.optional_floats}")

    # categorical standardization 
    for col, style in table.text_case.items():
        df[col] = getattr(df[col].str, style)()
    if table.text_case:
        log_stage(logger, stage, f"{name}: standardized text case on {list(table.text_case.keys())}")

    # custom sanity checks
    for reason, fn in table.checks:
        mask = pd.Series(fn(df), index=df.index).fillna(False).astype(bool)
        if mask.any():
            quarantine.add(df[mask], name, reason)
            log_stage(logger, stage, f"{name}: quarantined {int(mask.sum())} rows failing check '{reason}'", "warning")
        df = df[~mask]

    # exact duplicate rows
    n_before = len(df)
    df = df.drop_duplicates(subset=table.columns)
    if n_before - len(df) and not quiet:
        log_stage(logger, stage, f"{name}: {n_before - len(df)} exact duplicate rows dropped")

    # duplicate keys 
    if table.destination == "sqlite":
        dup = df.duplicated(table.unique_key_columns, keep="first")
        if dup.any():
            quarantine.add(df[dup], name, f"duplicate key {table.unique_key_columns}")
            log_stage(logger, stage, f"{name}: quarantined {int(dup.sum())} rows with duplicate key {table.unique_key_columns}", "warning")
        df = df[~dup]

    # referential integrity 
    for cols, parent_name in table.references:
        parent = parents.get(parent_name)
        if parent is None:
            log_stage(logger, stage, f"{name}: skipped referential check against '{parent_name}' — parent not loaded", "warning")
            continue
        valid = set(zip(*(parent[c] for c in cols)))
        orphan = pd.Series(
            [k not in valid for k in zip(*(df[c] for c in cols))],
            index=df.index, dtype=bool,
        )
        if orphan.any():
            quarantine.add(df[orphan], name, f"orphan: {cols} not in {parent_name}")
            log_stage(logger, stage, f"{name}: quarantined {int(orphan.sum())} orphan rows (missing in '{parent_name}')", "warning")
        df = df[~orphan]

    df = df.reset_index(drop=True)

    if not quiet:
        left = {c: int(v) for c, v in df[table.columns].isna().sum().items() if v}
        summary = f"{name}: in={n_in} out={len(df)} removed={n_in - len(df)}"
        log_stage(logger, stage, f"{summary} | remaining NULLs (nullable, kept): {left or 'none'}")

    return df

# ------ Verify ------

def verify_sqlite(sqlite_db_path: str, expected: dict[str, int], tables: list[TableConfig], logger: logging.Logger) -> None:
    """Prove the SQLite warehouse is correct. Raises an error if any check fails.
        Check 1 - row counts : rows in each table == rows we loaded
        Check 2 - foreign keys: no child row points to a parent that does not exist
        Check 3 - integrity   : the database file is not corrupted
    """
    stage = "Verify SQLite"
    problems = []
    conn = sqlite3.connect(sqlite_db_path)
    name_to_table = {Path(t.source_file).stem: t.output_name for t in tables}

    try:
        for table, n_loaded in expected.items():                       # Check 1
            n_in_db = conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
            passed = n_in_db == n_loaded
            log_stage(logger, stage, f"[{'PASS' if passed else 'FAIL'}] row count {table:<26}" f"loaded={n_loaded}  in database={n_in_db}")
            if not passed:
                problems.append(f"{table} row count")

            total_orphans = 0                                          # Check 2
            for t in tables:
                if t.destination != "sqlite" or not t.references:
                    continue
                for cols, parent_name in t.references:
                    parent_table = name_to_table.get(parent_name)
                    if parent_table is None:
                        continue
                    join_clause = " AND ".join(f"f.{c} = d.{c}" for c in cols)
                    where_clause = " AND ".join(f"d.{c} IS NULL" for c in cols)
                    sql = f"""
                        SELECT COUNT(*) FROM {t.output_name} f
                        LEFT JOIN {parent_table} d ON {join_clause}
                        WHERE {where_clause}
                    """
                    orphans = conn.execute(sql).fetchone()[0]
                    passed = orphans == 0
                    log_stage(
                        logger, stage,
                        f"[{'PASS' if passed else 'FAIL'}] foreign key {t.output_name} -> {parent_table}: {orphans} orphan rows",
                    )
                    if not passed:
                        problems.append(f"{t.output_name} -> {parent_table} orphans")
                    total_orphans += orphans

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
        "document count matches inserted rows": in_db >= stats["inserted"],
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
def _rows_for_sqlite(df: pd.DataFrame, columns: list[str]) -> list[tuple]:
    sub = df[columns].astype(object)
    sub = sub.where(sub.notna(), None)
    sub = sub.map(lambda v: None if v is pd.NA else v)
    return [tuple(row) for row in sub.itertuples(index=False, name=None)]

def load_table_to_sqlite(df: pd.DataFrame, table: TableConfig, sqlite_db_path: str, logger: logging.Logger) -> None:
    stage = f"{table.output_name} -> SQLite"

    if df.empty:
        log_stage(logger, stage, "No rows to load - skipping", level="warning")
        return

    os.makedirs(os.path.dirname(sqlite_db_path), exist_ok=True)
    conn = sqlite3.connect(sqlite_db_path)
    cur = conn.cursor()

    try:
        _create_table_if_not_exists(cur, table, df)

        placeholders = ", ".join(["?"] * len(table.columns))
        col_list = ", ".join(table.columns)
        conflict_cols = ", ".join(table.unique_key_columns)
        update_cols = [c for c in table.columns if c not in table.unique_key_columns]
        update_clause = ", ".join(f"{c}=excluded.{c}" for c in update_cols)

        sql = f"""
            INSERT INTO {table.output_name} ({col_list})
            VALUES ({placeholders})
            ON CONFLICT({conflict_cols}) DO UPDATE SET {update_clause}
        """

        rows = _rows_for_sqlite(df, table.columns)
        cur.executemany(sql, rows)
        conn.commit()

        log_stage(logger, stage, f"Upserted {len(rows)} rows into '{table.output_name}'")
    except Exception as e:
        conn.rollback()
        log_stage(logger, stage, f"Failed to load into SQLite: {e}", level="error")
        raise
    finally:
        conn.close()

def _create_table_if_not_exists(cur, table: TableConfig, df: pd.DataFrame) -> None:
    type_map = {"Int64": "INTEGER", "Float64": "REAL", "string": "TEXT", "object": "TEXT"}
    col_defs = []
    for col in table.columns:
        dtype = str(df[col].dtype)
        sql_type = type_map.get(dtype, "TEXT")
        col_defs.append(f"{col} {sql_type}")

    unique_clause = f", UNIQUE({', '.join(table.unique_key_columns)})"
    ddl = f"CREATE TABLE IF NOT EXISTS {table.output_name} ({', '.join(col_defs)}{unique_clause})"
    cur.execute(ddl)

def load_clickstream_to_mongo(
    path: str, db_name: str, collection_name: str, mongo_uri: str, unique_key_columns: list[str], quarantine_dir: str, 
    logger: logging.Logger, table: TableConfig, parents: dict[str, pd.DataFrame], quarantine: Quarantine, chunk_size: int = 50_000
):
    stage = "Clickstream to MongoDB"
    source_file = os.path.basename(path)

    log_stage(logger, stage, f"Starting load of {source_file} into '{collection_name}'")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000, socketTimeoutMS=20000)
    collection = client[db_name][collection_name]
    log_stage(logger, stage, "Creating unique index...")
    collection.create_index([(col, 1) for col in unique_key_columns], unique=True, name="uniq_studentvle_event")
    log_stage(logger, stage, "Index created")
    
    rows_read = 0
    rows_valid = 0
    total_inserted = 0
    total_skipped = 0

    try:
        for chunk_idx, chunk in enumerate(pd.read_csv(path, chunksize=chunk_size)):
            rows_read += len(chunk)

            clean_chunk = transform_table(chunk, table, logger, parents, quarantine)
            rows_valid += len(clean_chunk)

            if clean_chunk.empty:
                continue

            docs = clean_chunk.to_dict(orient="records")
            for i, doc in enumerate(docs):
                doc["_source_file"] = source_file
                doc["_source_line"] = chunk_idx * chunk_size + i

            try:
                result = collection.insert_many(docs, ordered=False)
                inserted = len(result.inserted_ids)
                total_inserted += inserted
                log_stage(logger, stage, f"Chunk {chunk_idx}: inserted {inserted}/{len(docs)} rows")
            except BulkWriteError as bwe:
                inserted = bwe.details.get("nInserted", 0)
                skipped = len(docs) - inserted
                total_inserted += inserted
                total_skipped += skipped
                log_stage(logger, stage, f"Chunk {chunk_idx}: inserted {inserted}, skipped {skipped} duplicates", level="warning")

    finally:
        client.close()

    stats = {
        "rows_read": rows_read,
        "rows_valid": rows_valid,
        "inserted": total_inserted,
        "skipped_duplicates": total_skipped,
    }
    log_stage(logger, stage, f"Finished: {stats}")
    return stats

# ------ Execution ------
def run_pipeline() -> dict:
    start_time = time.perf_counter()
    cfg = get_config()

    logger = setup_logger(cfg.LOG_PATH)
    log_stage(logger, "Pipeline", "Pipeline run started")

    quarantine = Quarantine(cfg.PROCESSED_PATH, logger)
    cleaned = {}
    expected_counts = {}
    mongo_stats = {}

    sqlite_db_path = os.path.join(cfg.SQLITE_OUTPUT_PATH, "warehouse.db")
    landing_zone = Path(cfg.LANDING_ZONE_PATH)

    for table in cfg.TABLES:
        file_path = landing_zone / table.source_file
        table_key = Path(table.source_file).stem

        if table.destination == "sqlite":
            # Extract
            raw_df = read_csv_file(file_path, logger)
            if raw_df is None:
                log_stage(logger, "Pipeline", f"Skipping {table.source_file} - extract failed or file empty", level="error")
                continue

            # Transform
            clean_df = transform_table(raw_df, table, logger, cleaned, quarantine)
            cleaned[table_key] = clean_df  

            if clean_df.empty:
                log_stage(logger, "Pipeline", f"Skipping load for {table.output_name} — no valid rows after transform", level="warning")
                expected_counts[table.output_name] = 0
                continue

            # Load
            load_table_to_sqlite(clean_df, table, sqlite_db_path, logger)
            expected_counts[table.output_name] = len(clean_df)

        elif table.destination == "mongo":
            stats = load_clickstream_to_mongo(
                str(file_path),
                cfg.MONGO_DB_NAME,
                table.output_name,
                cfg.MONGO_URI,
                table.unique_key_columns,
                cfg.PROCESSED_PATH,
                logger,
                table,
                cleaned,
                quarantine,
            )
            mongo_stats[table.output_name] = stats

    # Verify 
    log_stage(logger, "Pipeline", "Extract/Transform/Load complete — starting verification")

    if expected_counts:
        verify_sqlite(sqlite_db_path, expected_counts, cfg.TABLES, logger)

    for table in cfg.TABLES:
        if table.destination == "mongo":
            stats = mongo_stats.get(table.output_name)
            if stats:
                verify_mongo(cfg, table, stats, logger)

    elapsed = time.perf_counter() - start_time
    log_stage(logger, "COMPLETE", f"Multi-target pipeline execution finished successfully in {elapsed:.2f} seconds.")
    
    return {
        "elapsed_seconds": round(elapsed, 2),
    }

if __name__ == "__main__":
    summary = run_pipeline()
    print(summary)