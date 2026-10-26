# Data Dictionary
Table | Column(s) | Data type | Business definition
--- | --- | --- | ---
dim_courses | code_module, code_presentation | TEXT | Composite key identifying a course presentation.
dim_courses | module_presentation_length | INTEGER | Course duration in days; must be positive.
dim_assessments | id_assessment | INTEGER | Unique assessment identifier.
dim_assessments | assessment_type, date, weight | TEXT, nullable INTEGER, REAL | Assessment type, optional day offset, and weight constrained to 0–100.
dim_vles | id_site | INTEGER | Unique virtual learning environment resource identifier.
dim_vles | activity_type, week_from, week_to | TEXT, nullable INTEGERs | Resource type and optional non-negative ordered week range.
dim_students | id_student, code_module, code_presentation | INTEGER, TEXT, TEXT | Composite key for a student in a course presentation.
dim_students | gender, region, highest_education, imd_band, age_band, disability, final_result | TEXT | Student attributes; gender, disability, and final result use allowed values.
dim_students | num_of_prev_attempts, studied_credits | INTEGER | Attempts are non-negative; studied credits are positive.
fact_registration | id_student, code_module, code_presentation | INTEGER, TEXT, TEXT | Composite key and foreign key to the student-course record.
fact_registration | date_registration, date_unregistration | Nullable INTEGER | Course-relative day offsets; unregistration cannot precede registration.
fact_assessment_results | id_assessment, id_student | INTEGER | Composite key for a student's assessment result.
fact_assessment_results | date_submitted, is_banked, score | Nullable INTEGER, INTEGER, nullable REAL | Submission day offset, binary banked flag, and optional score from 0–100.