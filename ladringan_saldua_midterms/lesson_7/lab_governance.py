"""
Python code establishing strict relational constraints (NOT NULL, CHECK constraints on positive 
quantities/prices) at the DDL table level.

A MongoDB schema validation snippet enforcing a strict $jsonSchema validator on collections to block 
malformed or non-compliant documents.

The governed database contains the constrained schema only; this script does not copy source rows.
"""
from __future__ import annotations

import sqlite3
from pathlib import Path

BASE_DIR = Path(__file__).resolve().parent
SOURCE_DB = BASE_DIR / "warehouse.db"
GOVERNED_DB = BASE_DIR / "governed_warehouse.db"
OUTPUT_DIR = BASE_DIR / "governance"
MONGO_VALIDATOR_FILE = OUTPUT_DIR / "mongo_schema_validation.js"

EXPECTED_SOURCE_TABLES = (
    "dim_courses", "dim_assessments", "dim_vles", "dim_students",
    "fact_registration", "fact_assessment_results",
)

DDL_STATEMENTS = (
    """CREATE TABLE IF NOT EXISTS dim_courses (
        code_module TEXT NOT NULL CHECK (length(trim(code_module)) > 0),
        code_presentation TEXT NOT NULL CHECK (length(trim(code_presentation)) > 0),
        module_presentation_length INTEGER NOT NULL CHECK (module_presentation_length > 0),
        PRIMARY KEY (code_module, code_presentation)
    )""",
    """CREATE TABLE IF NOT EXISTS dim_assessments (
        id_assessment INTEGER PRIMARY KEY,
        code_module TEXT NOT NULL CHECK (length(trim(code_module)) > 0),
        code_presentation TEXT NOT NULL CHECK (length(trim(code_presentation)) > 0),
        assessment_type TEXT NOT NULL CHECK (length(trim(assessment_type)) > 0),
        date INTEGER,
        weight REAL NOT NULL CHECK (weight >= 0 AND weight <= 100),
        FOREIGN KEY (code_module, code_presentation)
          REFERENCES dim_courses (code_module, code_presentation)
    )""",
    """CREATE TABLE IF NOT EXISTS dim_vles (
        id_site INTEGER PRIMARY KEY,
        code_module TEXT NOT NULL CHECK (length(trim(code_module)) > 0),
        code_presentation TEXT NOT NULL CHECK (length(trim(code_presentation)) > 0),
        activity_type TEXT NOT NULL CHECK (length(trim(activity_type)) > 0),
        week_from INTEGER,
        week_to INTEGER,
        CHECK (week_from IS NULL OR week_from >= 0),
        CHECK (week_to IS NULL OR week_to >= 0),
        CHECK (week_from IS NULL OR week_to IS NULL OR week_to >= week_from),
        FOREIGN KEY (code_module, code_presentation)
          REFERENCES dim_courses (code_module, code_presentation)
    )""",
    """CREATE TABLE IF NOT EXISTS dim_students (
        id_student INTEGER NOT NULL CHECK (id_student > 0),
        code_module TEXT NOT NULL CHECK (length(trim(code_module)) > 0),
        code_presentation TEXT NOT NULL CHECK (length(trim(code_presentation)) > 0),
        gender TEXT NOT NULL CHECK (gender IN ('M', 'F')),
        region TEXT NOT NULL CHECK (length(trim(region)) > 0),
        highest_education TEXT NOT NULL CHECK (length(trim(highest_education)) > 0),
        imd_band TEXT,
        age_band TEXT NOT NULL CHECK (length(trim(age_band)) > 0),
        num_of_prev_attempts INTEGER NOT NULL CHECK (num_of_prev_attempts >= 0),
        studied_credits INTEGER NOT NULL CHECK (studied_credits > 0),
        disability TEXT NOT NULL CHECK (disability IN ('Y', 'N')),
        final_result TEXT NOT NULL CHECK (final_result IN ('Pass', 'Distinction', 'Fail', 'Withdrawn')),
        PRIMARY KEY (id_student, code_module, code_presentation),
        FOREIGN KEY (code_module, code_presentation)
          REFERENCES dim_courses (code_module, code_presentation)
    )""",
    """CREATE TABLE IF NOT EXISTS fact_registration (
        id_student INTEGER NOT NULL CHECK (id_student > 0),
        code_module TEXT NOT NULL CHECK (length(trim(code_module)) > 0),
        code_presentation TEXT NOT NULL CHECK (length(trim(code_presentation)) > 0),
        date_registration INTEGER,
        date_unregistration INTEGER,
        PRIMARY KEY (id_student, code_module, code_presentation),
        CHECK (date_registration IS NULL OR date_unregistration IS NULL OR date_unregistration >= date_registration),
        FOREIGN KEY (id_student, code_module, code_presentation)
          REFERENCES dim_students (id_student, code_module, code_presentation)
    )""",
    """CREATE TABLE IF NOT EXISTS fact_assessment_results (
        id_assessment INTEGER NOT NULL,
        id_student INTEGER NOT NULL CHECK (id_student > 0),
        date_submitted INTEGER,
        is_banked INTEGER NOT NULL CHECK (is_banked IN (0, 1)),
        score REAL,
        PRIMARY KEY (id_assessment, id_student),
        CHECK (score IS NULL OR (score >= 0 AND score <= 100)),
        FOREIGN KEY (id_assessment) REFERENCES dim_assessments (id_assessment)
    )""",
)

INDEX_STATEMENTS = (
    "CREATE INDEX IF NOT EXISTS idx_assessments_course ON dim_assessments (code_module, code_presentation)",
    "CREATE INDEX IF NOT EXISTS idx_vles_course ON dim_vles (code_module, code_presentation)",
    "CREATE INDEX IF NOT EXISTS idx_students_course_result ON dim_students (code_module, code_presentation, final_result)",
    "CREATE INDEX IF NOT EXISTS idx_registration_course_date ON fact_registration (code_module, code_presentation, date_registration)",
    "CREATE INDEX IF NOT EXISTS idx_results_student ON fact_assessment_results (id_student)",
)

# rejects unknown fields and invalid/missing required fields.
MONGO_VALIDATOR = r'''
db.createCollection("assessment_result_documents", {
  validator: {
    $jsonSchema: {
      bsonType: "object",
      required: [
        "id_assessment",
        "id_student",
        "is_banked"
      ],
      additionalProperties: false,
      properties: {
        _id: {
          bsonType: "objectId"
        },
        id_assessment: {
          bsonType: ["int", "long"],
          minimum: 1
        },
        id_student: {
          bsonType: ["int", "long"],
          minimum: 1
        },
        date_submitted: {
          bsonType: ["int", "long", "null"]
        },
        is_banked: {
          bsonType: ["int", "long"],
          enum: [0, 1]
        },
        score: {
          bsonType: ["double", "int", "long", "decimal", "null"],
          minimum: 0,
          maximum: 100
        },
        course: {
          bsonType: "object",
          required: [
            "code_module",
            "code_presentation"
          ],
          additionalProperties: false,
          properties: {
            code_module: {
              bsonType: "string",
              minLength: 1
            },
            code_presentation: {
              bsonType: "string",
              minLength: 1
            }
          }
        },
        metadata: {
          bsonType: "object"
        }
      }
    }
  },
  validationLevel: "strict",
  validationAction: "error"
});
'''


def connect_db(path: Path) -> sqlite3.Connection:
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn


def validate_source_schema(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(
            f"Source warehouse not found: {path}. Update SOURCE_DB in this script if needed."
        )
    with connect_db(path) as conn:
        tables = {row[0] for row in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
        )}
        missing = sorted(set(EXPECTED_SOURCE_TABLES) - tables)
        if missing:
            raise RuntimeError("Missing expected source tables: " + ", ".join(missing))
        print(f"[source] {path}")
        for table in EXPECTED_SOURCE_TABLES:
            count = conn.execute(f'SELECT COUNT(*) FROM "{table}"').fetchone()[0]
            print(f"[source rows] {table}: {count:,}")


def create_governed_schema(path: Path) -> None:
    with connect_db(path) as conn:
        for statement in DDL_STATEMENTS:
            conn.execute(statement)
            print("[DDL] Created/verified constrained table.")
        for statement in INDEX_STATEMENTS:
            conn.execute(statement)
            print(f"[INDEX] {statement};")
        conn.commit()
        print(f"[governed] Schema created in {path}")
        for row in conn.execute(
            "SELECT name, sql FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%' ORDER BY name"
        ):
            print(f"\n--- {row['name']} ---\n{row['sql'].strip()}")


def write_mongodb_validator(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(MONGO_VALIDATOR, encoding="utf-8")
    print(f"[MongoDB] Validator snippet written to {path}")

def main() -> None:
    print("=== Database Governance Lab ===")
    validate_source_schema(SOURCE_DB)
    create_governed_schema(GOVERNED_DB)
    write_mongodb_validator(MONGO_VALIDATOR_FILE)

if __name__ == "__main__":
    main()
