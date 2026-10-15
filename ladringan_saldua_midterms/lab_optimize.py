import os
import shutil
import sqlite3
from datetime import datetime
from pathlib import Path

# Database optimization shiz

DB_PATH = Path("data/SQLite/warehouse.db")
BACKUP_DIR = Path("backups")


def connect_db(db_path: Path) -> sqlite3.Connection:
    # Open the warehouse
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    return conn


def set_pragmas(conn: sqlite3.Connection) -> None:
    # Set SQLite options
    pragmas = {
        "PRAGMA foreign_keys = ON;": "foreign_keys",
        "PRAGMA journal_mode = WAL;": "journal_mode",
        "PRAGMA synchronous = NORMAL;": "synchronous",
        "PRAGMA cache_size = -2000;": "cache_size",
        "PRAGMA temp_store = MEMORY;": "temp_store",
    }
    for stmt, label in pragmas.items():
        try:
            result = conn.execute(stmt).fetchone()
            value = result[0] if isinstance(result, tuple) else result
            print(f"[{label}] {value if value is not None else 'ok'}")
        except Exception as exc:
            print(f"[{label}] failed: {exc}")


def explain_query_plan(conn: sqlite3.Connection, label: str, query: str) -> None:
    # Show query plan
    print(f"\n=== {label} ===")
    print(query)
    print("\nEXPLAIN QUERY PLAN:")
    for row in conn.execute(f"EXPLAIN QUERY PLAN {query}"):
        print(tuple(row))


def backup_database(db_path: Path, backup_dir: Path) -> Path:
    # Save a backup
    backup_dir.mkdir(parents=True, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup_path = backup_dir / f"{db_path.stem}_backup_{timestamp}.db"
    shutil.copy2(db_path, backup_path)
    print(f"\n[backup] Created backup at {backup_path}")
    return backup_path


def create_indexes(conn: sqlite3.Connection) -> None:
    # Add query indexes
    indexes = [
        "CREATE INDEX IF NOT EXISTS idx_dim_students_final_result ON dim_students(final_result, code_module, code_presentation, id_student);",
        "CREATE INDEX IF NOT EXISTS idx_dim_assessments_type_date ON dim_assessments(assessment_type, date, id_assessment);",
        "CREATE INDEX IF NOT EXISTS idx_fact_assessment_results_student_assessment ON fact_assessment_results(id_student, id_assessment, score);",
        "CREATE INDEX IF NOT EXISTS idx_fact_registration_date_module ON fact_registration(date_registration, code_module, code_presentation, id_student);",
        "CREATE INDEX IF NOT EXISTS idx_dim_courses_module_presentation ON dim_courses(code_module, code_presentation);",
    ]

    for stmt in indexes:
        conn.execute(stmt)
        print(f"[index] {stmt}")

    conn.commit()


def run_before_query(conn: sqlite3.Connection) -> None:
    # Run baseline query
    query = """
        SELECT s.code_module, s.code_presentation, s.id_student,
               AVG(f.score) AS avg_score,
               COUNT(*) AS assessment_count
        FROM dim_students s
        JOIN fact_assessment_results f ON s.id_student = f.id_student
        JOIN dim_assessments a ON f.id_assessment = a.id_assessment
        WHERE s.final_result = 'Pass'
          AND a.assessment_type = 'TMA'
        GROUP BY s.code_module, s.code_presentation, s.id_student
        ORDER BY avg_score DESC
        LIMIT 20;
    """
    explain_query_plan(conn, "Before indexes: pass-rate analysis by TMA assessments", query)
    rows = conn.execute(query).fetchall()
    print(f"\n[query result] {len(rows)} rows returned")
    for row in rows[:5]:
        print(dict(row))


def run_after_query(conn: sqlite3.Connection) -> None:
    # Check indexed query >.>
    query = """
        SELECT s.code_module, s.code_presentation, s.id_student,
               AVG(f.score) AS avg_score,
               COUNT(*) AS assessment_count
        FROM dim_students s
        JOIN fact_assessment_results f ON s.id_student = f.id_student
        JOIN dim_assessments a ON f.id_assessment = a.id_assessment
        WHERE s.final_result = 'Pass'
          AND a.assessment_type = 'TMA'
        GROUP BY s.code_module, s.code_presentation, s.id_student
        ORDER BY avg_score DESC
        LIMIT 20;
    """
    explain_query_plan(conn, "After indexes: pass-rate analysis by TMA assessments", query)
    rows = conn.execute(query).fetchall()
    print(f"\n[query result] {len(rows)} rows returned")
    for row in rows[:5]:
        print(dict(row))


def run_secondary_query(conn: sqlite3.Connection) -> None:
    # Check course registrations
    query = """
        SELECT code_module, code_presentation, COUNT(*) AS registrations
        FROM fact_registration
        WHERE date_registration IS NOT NULL
        GROUP BY code_module, code_presentation
        ORDER BY registrations DESC
        LIMIT 10;
    """
    explain_query_plan(conn, "Before/after: registration counts by course", query)
    rows = conn.execute(query).fetchall()
    print(f"\n[secondary query result] {len(rows)} rows returned")
    for row in rows[:5]:
        print(dict(row))


def main() -> None:
    # Run optimization steps ou shi
    db_path = DB_PATH
    if not db_path.exists():
        raise FileNotFoundError(f"Database not found: {db_path}")

    conn = connect_db(db_path)
    try:
        print("=== SQLite Optimization Lab ===")
        print(f"Database: {db_path.resolve()}")

        set_pragmas(conn)
        print("\n--- BEFORE INDEXES ---")
        run_before_query(conn)
        run_secondary_query(conn)

        print("\n--- INDEX CREATION ---")
        create_indexes(conn)

        print("\n--- AFTER INDEXES ---")
        run_after_query(conn)
        run_secondary_query(conn)

        backup_database(db_path, BACKUP_DIR)

        print("\n=== Final row counts ===")
        for table_name, in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%' ORDER BY name;"
        ):
            count = conn.execute(f"SELECT COUNT(*) FROM \"{table_name}\"").fetchone()[0]
            print(f"{table_name}: {count}")

    finally:
        conn.close()


if __name__ == "__main__":
    main()
