from __future__ import annotations
import pandas as pd
import sqlite3
from pymongo import MongoClient
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import sys
from dotenv import load_dotenv
import os
import json


# ------ Config ------
def get_uri():
    load_dotenv()
    mongo_uri = os.getenv("MONGODB_URI")
    if not mongo_uri:
        raise RuntimeError(
            "MONGO_URI is not set. Copy .env.example to .env and set your MongoDB connection string."
        )
    return mongo_uri

class OLAPConfig:
    MONGO_URI = get_uri()
    DB_PATH = "data/SQLite/warehouse.db"
    OUTPUT_DIR = "data/OLAP"
    OUTPUT_FILE = "olap_output.txt"
    FACETS_JSON_PATH = "data/OLAP/olap_facets.json"
    
    # Analysis queries with window functions
    QUERIES = {
        "student_progress_timeline": """
            WITH ranked_assessments AS (
                SELECT 
                    s.id_student,
                    s.code_module,
                    s.code_presentation,
                    a.id_assessment,
                    a.assessment_type,
                    a.date AS assessment_due_date,
                    r.date_submitted,
                    r.score,
                    a.weight,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY a.date
                    ) AS assessment_sequence
                FROM dim_students s
                JOIN v_assessment_results r ON s.id_student = r.id_student
                    AND s.code_module = r.code_module
                    AND s.code_presentation = r.code_presentation
                JOIN dim_assessments a ON r.id_assessment = a.id_assessment
            ),
            progress_metrics AS (
                SELECT 
                    id_student,
                    code_module,
                    code_presentation,
                    assessment_sequence,
                    assessment_due_date,
                    date_submitted,
                    score,
                    weight,
                    LAG(score, 1) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY assessment_sequence
                    ) AS previous_score,
                    LAG(date_submitted, 1) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY assessment_sequence
                    ) AS previous_submission_date,
                    SUM(score) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY assessment_sequence
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_score,
                    SUM(weight) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY assessment_sequence
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS cumulative_weight,
                    AVG(score) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY assessment_sequence
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_average,
                    COUNT(*) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY assessment_sequence
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS assessments_completed
                FROM ranked_assessments
            )
            SELECT 
                id_student,
                code_module,
                code_presentation,
                assessment_sequence,
                assessment_due_date,
                date_submitted,
                score,
                previous_score,
                score - COALESCE(previous_score, score) AS score_change,
                cumulative_score,
                cumulative_weight,
                running_average,
                assessments_completed,
                CASE 
                    WHEN previous_submission_date IS NOT NULL 
                    THEN date_submitted - previous_submission_date 
                    ELSE NULL 
                END AS days_between_submissions
            FROM progress_metrics
            ORDER BY id_student, code_module, code_presentation, assessment_sequence
            LIMIT 50;
        """,
        
        "course_performance_summary": """
            WITH course_stats AS (
                SELECT 
                    s.code_module,
                    s.code_presentation,
                    s.id_student,
                    s.final_result,
                    COUNT(r.id_assessment) AS total_assessments,
                    AVG(r.score) AS avg_score,
                    SUM(r.score) AS total_score,
                    MAX(r.score) AS max_score,
                    MIN(r.score) AS min_score,
                    SQRT(AVG(r.score*r.score) - AVG(r.score)*AVG(r.score)) AS score_stddev,
                    RANK() OVER (
                        PARTITION BY s.code_module, s.code_presentation 
                        ORDER BY AVG(r.score) DESC
                    ) AS class_rank,
                    COUNT(*) OVER (
                        PARTITION BY s.code_module, s.code_presentation
                    ) AS total_students_in_course
                FROM dim_students s
                LEFT JOIN v_assessment_results r ON s.id_student = r.id_student
                    AND s.code_module = r.code_module
                    AND s.code_presentation = r.code_presentation
                GROUP BY s.code_module, s.code_presentation, s.id_student, s.final_result
            )
            SELECT 
                code_module,
                code_presentation,
                id_student,
                final_result,
                total_assessments,
                ROUND(avg_score, 2) AS avg_score,
                total_score,
                max_score,
                min_score,
                ROUND(score_stddev, 2) AS score_stddev,
                class_rank,
                total_students_in_course,
                ROUND(100.0 * class_rank / total_students_in_course, 2) AS percentile_rank
            FROM course_stats
            ORDER BY code_module, code_presentation, class_rank
            LIMIT 30;
        """,
        
        "assessment_submission_patterns": """
            WITH submission_analysis AS (
                SELECT 
                    s.code_module,
                    s.code_presentation,
                    a.id_assessment,
                    a.assessment_type,
                    a.date AS due_date,
                    a.weight,
                    r.date_submitted,
                    r.score,
                    CASE 
                        WHEN r.date_submitted <= a.date THEN 'on_time'
                        WHEN r.date_submitted > a.date THEN 'late'
                        ELSE 'unknown'
                    END AS submission_status,
                    CASE 
                        WHEN a.date > 0 THEN r.date_submitted - a.date
                        ELSE NULL 
                    END AS days_late,
                    COUNT(*) OVER (
                        PARTITION BY s.code_module, s.code_presentation, a.id_assessment
                    ) AS total_submissions,
                    AVG(r.score) OVER (
                        PARTITION BY s.code_module, s.code_presentation, a.id_assessment
                    ) AS avg_assessment_score,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.code_module, s.code_presentation, a.id_assessment
                        ORDER BY r.date_submitted
                    ) AS submission_order
                FROM dim_students s
                JOIN v_assessment_results r ON s.id_student = r.id_student
                    AND s.code_module = r.code_module
                    AND s.code_presentation = r.code_presentation
                JOIN dim_assessments a ON r.id_assessment = a.id_assessment
            )
            SELECT 
                code_module,
                code_presentation,
                id_assessment,
                assessment_type,
                due_date,
                weight,
                submission_status,
                days_late,
                score,
                avg_assessment_score,
                total_submissions,
                submission_order,
                ROUND(100.0 * submission_order / total_submissions, 2) AS submission_percentile
            FROM submission_analysis
            ORDER BY code_module, code_presentation, id_assessment, submission_order
            LIMIT 40;
        """,
        
        "student_trajectory_analysis": """
            WITH student_baseline AS (
                SELECT 
                    s.id_student,
                    s.code_module,
                    s.code_presentation,
                    s.final_result,
                    s.num_of_prev_attempts,
                    s.studied_credits,
                    COUNT(r.id_assessment) AS assessment_count,
                    AVG(r.score) AS overall_avg_score
                FROM dim_students s
                LEFT JOIN v_assessment_results r ON s.id_student = r.id_student
                    AND s.code_module = r.code_module
                    AND s.code_presentation = r.code_presentation
                GROUP BY s.id_student, s.code_module, s.code_presentation, s.final_result, 
                         s.num_of_prev_attempts, s.studied_credits
            ),
            assessment_trend AS (
                SELECT 
                    s.id_student,
                    s.code_module,
                    s.code_presentation,
                    a.date AS assessment_date,
                    r.score,
                    ROW_NUMBER() OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY a.date
                    ) AS time_order,
                    FIRST_VALUE(r.score) OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY a.date
                    ) AS first_score,
                    LAST_VALUE(r.score) OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY a.date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                    ) AS last_score,
                    SUM(r.score) OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY a.date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_total,
                    COUNT(*) OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY a.date
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS running_count
                FROM dim_students s
                JOIN v_assessment_results r ON s.id_student = r.id_student
                    AND s.code_module = r.code_module
                    AND s.code_presentation = r.code_presentation
                JOIN dim_assessments a ON r.id_assessment = a.id_assessment
            )
            SELECT 
                b.id_student,
                b.code_module,
                b.code_presentation,
                b.final_result,
                b.num_of_prev_attempts,
                b.studied_credits,
                b.assessment_count,
                ROUND(b.overall_avg_score, 2) AS overall_avg_score,
                t.time_order,
                t.assessment_date,
                t.score,
                t.first_score,
                t.last_score,
                t.score - t.first_score AS score_improvement,
                ROUND(t.running_total / NULLIF(t.running_count, 0), 2) AS running_avg,
                CASE 
                    WHEN t.last_score > t.first_score THEN 'improving'
                    WHEN t.last_score < t.first_score THEN 'declining'
                    ELSE 'stable'
                END AS trajectory_trend
            FROM student_baseline b
            JOIN assessment_trend t ON b.id_student = t.id_student
                AND b.code_module = t.code_module
                AND b.code_presentation = t.code_presentation
            ORDER BY b.id_student, b.code_module, b.code_presentation, t.time_order
            LIMIT 30;
        """,
        
        "time_based_engagement": """
            WITH time_windows AS (
                SELECT 
                    s.id_student,
                    s.code_module,
                    s.code_presentation,
                    r.date_submitted,
                    r.score,
                    a.date AS assessment_date,
                    CASE 
                        WHEN a.date > 0 
                        THEN CAST(r.date_submitted / 7 AS INTEGER)  -- Week number
                        ELSE NULL 
                    END AS submission_week,
                    DENSE_RANK() OVER (
                        PARTITION BY s.id_student, s.code_module, s.code_presentation 
                        ORDER BY r.date_submitted
                    ) AS engagement_sequence
                FROM dim_students s
                JOIN v_assessment_results r ON s.id_student = r.id_student
                    AND s.code_module = r.code_module
                    AND s.code_presentation = r.code_presentation
                JOIN dim_assessments a ON r.id_assessment = a.id_assessment
            ),
            weekly_metrics AS (
                SELECT 
                    id_student,
                    code_module,
                    code_presentation,
                    submission_week,
                    engagement_sequence,
                    score,
                    AVG(score) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY submission_week
                        ROWS BETWEEN 3 PRECEDING AND CURRENT ROW
                    ) AS rolling_4week_avg,
                    COUNT(*) OVER (
                        PARTITION BY id_student, code_module, code_presentation 
                        ORDER BY submission_week
                        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
                    ) AS total_engagements,
                    MIN(score) OVER (
                        PARTITION BY id_student, code_module, code_presentation
                    ) AS all_time_min,
                    MAX(score) OVER (
                        PARTITION BY id_student, code_module, code_presentation
                    ) AS all_time_max
                FROM time_windows
            )
            SELECT 
                id_student,
                code_module,
                code_presentation,
                submission_week,
                engagement_sequence,
                score,
                ROUND(rolling_4week_avg, 2) AS rolling_4week_avg,
                total_engagements,
                all_time_min,
                all_time_max,
                ROUND(100.0 * (score - all_time_min) / NULLIF(all_time_max - all_time_min, 0), 2) AS performance_percentile
            FROM weekly_metrics
            WHERE submission_week IS NOT NULL
            ORDER BY id_student, code_module, code_presentation, submission_week
            LIMIT 40;
        """,
        "retention_by_early_engagement": """
            WITH student_eng AS (
                SELECT b.id_student, v.code_module, v.code_presentation,
                    SUM(b.early_clicks) AS early_clicks
                FROM agg_bucket_engagement b
                JOIN dim_vles v ON v.id_site = b.id_site
                GROUP BY b.id_student, v.code_module, v.code_presentation
            ),
            banded AS (
                SELECT s.code_module, s.code_presentation, s.final_result,
                    NTILE(4) OVER (PARTITION BY s.code_module, s.code_presentation
                                    ORDER BY COALESCE(e.early_clicks, 0)) AS early_quartile
                FROM dim_students s
                LEFT JOIN student_eng e ON e.id_student = s.id_student
                    AND e.code_module = s.code_module
                    AND e.code_presentation = s.code_presentation
            )
            SELECT code_module, code_presentation, early_quartile,
                COUNT(*) AS students,
                ROUND(100.0 * SUM(final_result <> 'Withdrawn') / COUNT(*), 1) AS retention_pct,
                ROUND(100.0 * SUM(final_result IN ('Pass','Distinction')) / COUNT(*), 1) AS pass_pct
            FROM banded
            GROUP BY code_module, code_presentation, early_quartile
            ORDER BY code_module, code_presentation, early_quartile
        """,
        "learning_path_lift": """ 
            WITH early AS (
                SELECT b.id_student, v.code_module, v.code_presentation, v.activity_type,
                    SUM(b.early_clicks) AS c
                FROM agg_bucket_engagement b JOIN dim_vles v ON v.id_site = b.id_site
                GROUP BY 1, 2, 3, 4 HAVING c > 0
            ),
            baseline AS (
                SELECT 100.0 * AVG(final_result IN ('Pass','Distinction')) AS pass_pct FROM dim_students
            )
            SELECT e.activity_type,
                COUNT(*) AS students_engaged,
                ROUND(100.0 * AVG(s.final_result IN ('Pass','Distinction')), 1) AS pass_pct_engaged,
                ROUND(100.0 * AVG(s.final_result IN ('Pass','Distinction')) - (SELECT pass_pct FROM baseline), 1) AS lift_vs_baseline
            FROM early e
            JOIN dim_students s ON s.id_student = e.id_student
                AND s.code_module = e.code_module AND s.code_presentation = e.code_presentation
            GROUP BY e.activity_type
            HAVING students_engaged >= 200
            ORDER BY lift_vs_baseline DESC;
        """
    }

SITE = {"$mod": ["$_id", 10_000_000]}
STUDENT = {"$toLong": {"$floor": {"$divide": ["$_id", 10_000_000]}}}
EARLY_DAYS = 28   # "early engagement" = first 4 weeks

bucket_metrics = [{"$project": {
    "_id": 0,
    "id_student": STUDENT,
    "id_site": SITE,
    "total_clicks": {"$sum": "$n"},
    "active_days": {"$size": "$d"},
    "first_day": {"$min": "$d"},
    "last_day": {"$max": "$d"},
    "early_clicks": {"$reduce": {
        "input": {"$range": [0, {"$size": "$d"}]},
        "initialValue": 0,
        "in": {"$cond": [
            {"$lt": [{"$arrayElemAt": ["$d", "$$this"]}, EARLY_DAYS]},
            {"$add": ["$$value", {"$arrayElemAt": ["$n", "$$this"]}]},
            "$$value"]}}},
}}]


# ------ Logging ------
def setup_logger(output_dir: str, output_file: str, level: str = "info") -> logging.Logger:
    """Setup logger with file and console handlers, mirroring ETL pipeline pattern"""
    log_dir = Path(output_dir)
    log_dir.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger("olap_analysis")
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))
    logger.handlers.clear()

    # File handler for output
    file_fmt = logging.Formatter("%(asctime)s | %(levelname)-8s | %(message)s")
    file_handler = logging.FileHandler(log_dir / output_file)
    file_handler.setFormatter(file_fmt)
    logger.addHandler(file_handler)

    # Console handler
    console_fmt = logging.Formatter("%(message)s")
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(console_fmt)
    logger.addHandler(console_handler)

    return logger

def log_stage(logger: logging.Logger, stage: str, message: str, level: str = "info") -> None:
    """Log a stage message with consistent formatting"""
    log_fn = getattr(logger, level.lower(), logger.info)
    log_fn(f"[{stage}] {message}")


# ------ Database Operations ------
def get_connection(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # Enable column access by name
    return conn

def execute_query(conn: sqlite3.Connection, query_name: str, query: str, logger: logging.Logger) -> List[Dict]:
    stage = f"Query: {query_name}"
    log_stage(logger, stage, "Executing OLAP query")
    
    try:
        cursor = conn.execute(query)
        results = [dict(row) for row in cursor.fetchall()]
        row_count = len(results)
        log_stage(logger, stage, f"Query completed successfully - {row_count} rows returned")
        return results
    except sqlite3.Error as e:
        log_stage(logger, stage, f"Query failed: {e}", level="error")
        raise

def format_results(results: List[Dict], max_rows: int = 10) -> str:
    if not results:
        return "No results returned"
    
    headers = list(results[0].keys())
    output = []
    
    # Header row
    header_line = " | ".join(str(h).ljust(15) for h in headers)
    separator = "-" * len(header_line)
    output.append(header_line)
    output.append(separator)
    
    # Data rows (limited for readability)
    for row in results[:max_rows]:
        row_line = " | ".join(str(row.get(h, "NULL")).ljust(15) for h in headers)
        output.append(row_line)
    
    if len(results) > max_rows:
        output.append(f"... ({len(results) - max_rows} more rows)")
    
    return "\n".join(output)

def ensure_views(db_path: str, logger: logging.Logger) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE VIEW IF NOT EXISTS v_assessment_results AS
        SELECT r.id_assessment, r.id_student, r.date_submitted, r.is_banked, r.score,
               a.code_module, a.code_presentation
        FROM fact_assessment_results r
        JOIN dim_assessments a ON a.id_assessment = r.id_assessment
    """)
    conn.commit()
    conn.close()
    log_stage(logger, "OLAP", "Ensured views exist")

def export_engagement_to_sqlite(coll, sqlite_path, logger, batch=50_000):
    Path(sqlite_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(sqlite_path)
    conn.execute("DROP TABLE IF EXISTS agg_bucket_engagement")
    conn.execute("""CREATE TABLE agg_bucket_engagement (
        id_student INTEGER, id_site INTEGER, total_clicks INTEGER,
        active_days INTEGER, first_day INTEGER, last_day INTEGER, early_clicks INTEGER)""")
    cur = coll.aggregate(bucket_metrics, allowDiskUse=True, batchSize=batch)
    rows, total = [], 0
    for doc in cur:
        rows.append((int(doc["id_student"]), int(doc["id_site"]), doc["total_clicks"],
                     doc["active_days"], doc["first_day"], doc["last_day"], doc["early_clicks"]))
        if len(rows) >= batch:
            conn.executemany("INSERT INTO agg_bucket_engagement VALUES (?,?,?,?,?,?,?)", rows)
            total += len(rows); rows = []
    if rows:
        conn.executemany("INSERT INTO agg_bucket_engagement VALUES (?,?,?,?,?,?,?)", rows)
        total += len(rows)
    conn.execute("CREATE INDEX idx_agg_site ON agg_bucket_engagement(id_site)")
    conn.commit(); conn.close()
    log_stage(logger, "OLAP", f"Exported {total} bucket metrics to agg_bucket_engagement")


# ------ Analysis Pipeline ------
def run_olap_analysis(config: OLAPConfig, logger: logging.Logger) -> Dict[str, List[Dict]]:
    stage = "OLAP Analysis"
    log_stage(logger, stage, "Starting OLAP analysis pipeline")
    
    results = {}
    
    # Verify database exists
    db_path = Path(config.DB_PATH)
    if not db_path.exists():
        log_stage(logger, stage, f"Database not found at {config.DB_PATH}", level="error")
        raise FileNotFoundError(f"Database not found: {config.DB_PATH}")
    
    log_stage(logger, stage, f"Connecting to database: {config.DB_PATH}")
    
    conn = get_connection(config.DB_PATH)
    
    try:
        # Execute each query
        for query_name, query in config.QUERIES.items():
            log_stage(logger, stage, f"Processing query: {query_name}")
            
            try:
                query_results = execute_query(conn, query_name, query, logger)
                results[query_name] = query_results
                
                # Log sample results
                formatted = format_results(query_results, max_rows=5)
                pd.DataFrame(query_results).to_csv(Path(config.OUTPUT_DIR) / f"{query_name}.csv", index=False)
                log_stage(logger, stage, f"Sample results for {query_name}:\n{formatted}")
                
            except Exception as e:
                log_stage(logger, stage, f"Failed to execute {query_name}: {e}", level="error")
                results[query_name] = None
                continue
        
        log_stage(logger, stage, f"Completed {len(results)} OLAP analysis queries")
        
    finally:
        conn.close()
        log_stage(logger, stage, "Database connection closed")

    return results

def run_olap_facets(coll):
    return list(coll.aggregate([{"$facet": {
        "totals": [{"$group": {"_id": None, "buckets": {"$sum": 1},
                               "clicks": {"$sum": {"$sum": "$n"}}}}],
        # engagement depth: how many active days do student-site pairs have?
        "engagement_depth": [{"$bucket": {
            "groupBy": {"$size": "$d"},
            "boundaries": [1, 2, 4, 8, 16, 32, 64, 1000],
            "default": "other",
            "output": {"pairs": {"$sum": 1}, "clicks": {"$sum": {"$sum": "$n"}}}}}],
        # weekly click curve (relative to course start, negative = pre-course)
        "weekly_clicks": [
            {"$project": {"p": {"$zip": {"inputs": ["$d", "$n"]}}}},
            {"$unwind": "$p"},
            {"$group": {"_id": {"$floor": {"$divide": [{"$arrayElemAt": ["$p", 0]}, 7]}},
                        "clicks": {"$sum": {"$arrayElemAt": ["$p", 1]}}}},
            {"$sort": {"_id": 1}}],
    }}], allowDiskUse=True))[0]

def write_facets_to_json(facets: Dict, output_path: str, logger: logging.Logger) -> None:
    stage = "JSON Export"
    log_stage(logger, stage, f"Writing facets results to {output_path}")
    
    # Prepare data for JSON serialization
    json_data = {
        "generated_at": datetime.now().isoformat(),
        "totals": facets.get("totals", []),
        "engagement_depth": facets.get("engagement_depth", []),
        "weekly_clicks": facets.get("weekly_clicks", [])
    }
    
    # Ensure output directory exists
    output_file = Path(output_path)
    output_file.parent.mkdir(parents=True, exist_ok=True)
    
    # Write to JSON
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(json_data, f, indent=2, default=str)
    
    log_stage(logger, stage, f"Successfully wrote facets data to {output_path}")

def generate_summary(results: Dict[str, List[Dict]], logger: logging.Logger) -> None:
    stage = "Summary"
    log_stage(logger, stage, "Generating analysis summary")
    
    total_rows = sum(len(r) if r else 0 for r in results.values())
    successful_queries = sum(1 for r in results.values() if r is not None)
    
    log_stage(logger, stage, f"Total queries executed: {len(results)}")
    log_stage(logger, stage, f"Successful queries: {successful_queries}")
    log_stage(logger, stage, f"Total rows analyzed: {total_rows}")
    
    # Query-specific summaries
    for query_name, query_results in results.items():
        if query_results:
            log_stage(logger, stage, f"{query_name}: {len(query_results)} rows")
        else:
            log_stage(logger, stage, f"{query_name}: No data or failed", level="warning")


# ------ Main Execution ------
def main() -> Dict:
    start_time = datetime.now()
    
    config = OLAPConfig()
    logger = setup_logger(config.OUTPUT_DIR, config.OUTPUT_FILE)
    
    stage = "Initialization"
    log_stage(logger, stage, "OLAP Analysis Pipeline started")
    log_stage(logger, stage, f"Output file: {config.OUTPUT_DIR}/{config.OUTPUT_FILE}")
    log_stage(logger, stage, f"Facets JSON: {config.FACETS_JSON_PATH}")
    
    try:
        # MongoDB to SQLite bridge
        client = MongoClient(config.MONGO_URI, serverSelectionTimeoutMS=10000)
        try:
            coll = client["VLE_Logs"]["clickstream_events"]
            export_engagement_to_sqlite(coll, config.DB_PATH, logger)
            ensure_views(config.DB_PATH, logger)

            facets = run_olap_facets(coll)
            weekly = pd.DataFrame(facets["weekly_clicks"]).rename(columns={"_id": "week"})

            conn = sqlite3.connect(config.DB_PATH)
            try:
                drop = pd.read_sql("""
                    SELECT CAST(date_unregistration / 7 AS INTEGER) AS week, COUNT(*) AS withdrawals
                    FROM fact_registration WHERE date_unregistration IS NOT NULL GROUP BY 1
                """, conn)
            finally:
                conn.close()

            curve = weekly.merge(drop, on="week", how="left").fillna({"withdrawals": 0})
            curve["clicks_per_withdrawal"] = curve["clicks"] / curve["withdrawals"].replace(0, pd.NA)
            
            # Write facets results to JSON
            write_facets_to_json(facets, config.FACETS_JSON_PATH, logger)
        finally:
            client.close()

        # Run SQLite OLAP analysis
        results = run_olap_analysis(config, logger)
        
        # Generate summary
        generate_summary(results, logger)
        
        # Completion
        elapsed = (datetime.now() - start_time).total_seconds()
        log_stage(logger, "COMPLETE", f"OLAP analysis completed successfully in {elapsed:.2f} seconds")
        
        return {
            "status": "success",
            "queries_executed": len(results),
            "successful_queries": sum(1 for r in results.values() if r is not None),
            "elapsed_seconds": round(elapsed, 2),
            "output_file": f"{config.OUTPUT_DIR}/{config.OUTPUT_FILE}",
            "facets_json": config.FACETS_JSON_PATH
        }
        
    except Exception as e:
        elapsed = (datetime.now() - start_time).total_seconds()
        log_stage(logger, "ERROR", f"OLAP analysis failed: {e}", level="error")
        log_stage(logger, "COMPLETE", f"Pipeline failed after {elapsed:.2f} seconds", level="error")
        
        return {
            "status": "error",
            "error": str(e),
            "elapsed_seconds": round(elapsed, 2)
        }
    

if __name__ == "__main__":
    summary = main()
    print("\n" + "="*50)
    print("OLAP Analysis Summary:")
    print("="*50)
    for key, value in summary.items():
        print(f"{key}: {value}")
