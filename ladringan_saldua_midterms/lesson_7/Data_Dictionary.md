# Lesson 7 Data Dictionary

This dictionary describes the SQLite warehouse in `clickstream_warehouse.db` and the clickstream document shape used by the project ETL.

## SQLite Warehouse

The data types below are the types declared in the current lesson database. Row counts are from the inspected database snapshot.

| Table | Column(s) | Data type | Business definition |
| --- | --- | --- | --- |
| `dim_courses` | `code_module`, `code_presentation` | TEXT | Course module and presentation identifiers; together form the logical course-presentation key. |
| `dim_courses` | `module_presentation_length` | TEXT | Length of the course presentation in days. The current database stores this value as TEXT. |
| `dim_assessments` | `id_assessment` | INTEGER | Unique assessment identifier and logical key. |
| `dim_assessments` | `code_module`, `code_presentation` | TEXT | Course presentation associated with the assessment. |
| `dim_assessments` | `assessment_type` | TEXT | Assessment category, such as TMA, CMA, or Exam. |
| `dim_assessments` | `date` | INTEGER | Assessment day offset from the course presentation start; may be unavailable. |
| `dim_assessments` | `weight` | REAL | Assessment contribution to the course grade, usually expressed as a percentage. |
| `dim_vles` | `id_site` | INTEGER | Unique identifier for a Virtual Learning Environment (VLE) resource; logical key. |
| `dim_vles` | `code_module`, `code_presentation` | TEXT | Course presentation containing the VLE resource. |
| `dim_vles` | `activity_type` | TEXT | Type of online learning resource or activity. |
| `dim_vles` | `week_from`, `week_to` | INTEGER | Optional first and last course weeks when the resource is available. |
| `dim_students` | `id_student` | INTEGER | Anonymized student identifier. |
| `dim_students` | `code_module`, `code_presentation` | TEXT | Course presentation associated with the student record. Together with `id_student`, forms the logical key. |
| `dim_students` | `gender`, `region`, `highest_education`, `imd_band`, `age_band`, `disability` | TEXT | Recorded demographic and background categories for the student. |
| `dim_students` | `num_of_prev_attempts` | INTEGER | Number of earlier attempts at the course. |
| `dim_students` | `studied_credits` | INTEGER | Number of credits the student is studying. |
| `dim_students` | `final_result` | TEXT | Final course outcome, such as Pass, Fail, Distinction, or Withdrawn. |
| `fact_registration` | `id_student` | INTEGER | Anonymized student identifier; joins to the student-course record. |
| `fact_registration` | `code_module`, `code_presentation` | TEXT | Registered course presentation. Combined with `id_student`, forms the logical key. |
| `fact_registration` | `date_registration`, `date_unregistration` | INTEGER | Registration and unregistration day offsets relative to the course presentation; either may be unavailable. |
| `fact_assessment_results` | `id_assessment`, `id_student` | INTEGER | Assessment and student identifiers; together form the logical result key. |
| `fact_assessment_results` | `date_submitted` | INTEGER | Submission day offset relative to the course presentation. |
| `fact_assessment_results` | `is_banked` | INTEGER | Indicator for a result carried over from an earlier attempt (0 or 1). |
| `fact_assessment_results` | `score` | REAL | Assessment score on a 0-100 scale; may be unavailable. |
| `agg_bucket_engagement` | `id_student`, `id_site` | INTEGER | Student and VLE resource identifiers for an engagement aggregate. No unique key is declared. |
| `agg_bucket_engagement` | `total_clicks`, `active_days`, `first_day`, `last_day`, `early_clicks` | INTEGER | Aggregated click count, number of active days, first and last activity offsets, and clicks in the early-engagement period. |

### Logical Keys and Relationships

| Table | Logical key | Relationship |
| --- | --- | --- |
| `dim_courses` | `code_module` + `code_presentation` | Parent course-presentation record for course dimensions and related facts. |
| `dim_assessments` | `id_assessment` | `code_module` + `code_presentation` identify its course presentation. |
| `dim_vles` | `id_site` | `code_module` + `code_presentation` identify its course presentation. |
| `dim_students` | `code_module` + `code_presentation` + `id_student` | Student record for a specific course presentation. |
| `fact_registration` | `code_module` + `code_presentation` + `id_student` | Joins to the corresponding student-course record. |
| `fact_assessment_results` | `id_assessment` + `id_student` | Joins to the assessment and student dimensions. |
| `agg_bucket_engagement` | Not declared | `id_student` and `id_site` relate to student and VLE records. |

The keys and relationships above describe the warehouse model. The current SQLite schema does not declare foreign-key constraints, and its generated tables do not declare `NOT NULL` or `CHECK` constraints. ETL validation or governance rules should not be described as database-enforced unless those rules have been applied and verified.

## MongoDB Clickstream

The ETL stores clickstream events in `VLE_Logs.clickstream_events` as compact documents grouped by student and VLE site. Date and click arrays are positionally aligned.

| Collection | Field | BSON type | Business definition |
| --- | --- | --- | --- |
| `clickstream_events` | `_id` | Integer | Generated bucket identifier based on the student and VLE site. |
| `clickstream_events` | `id_site` | Integer | VLE resource identifier; relates to `dim_vles.id_site`. |
| `clickstream_events` | `d` | Array of integers | Activity day offsets for the bucket. |
| `clickstream_events` | `n` | Array of integers | Click counts corresponding by position to the values in `d`. |

## Snapshot Counts

| Table | Rows |
| --- | ---: |
| `dim_courses` | 22 |
| `dim_assessments` | 195 |
| `dim_vles` | 6,364 |
| `dim_students` | 32,593 |
| `fact_registration` | 32,593 |
| `fact_assessment_results` | 171,047 |
| `agg_bucket_engagement` | 0 |
