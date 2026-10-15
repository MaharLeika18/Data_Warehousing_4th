"""
A Python script containing explicit SQL DDL statements that apply single-column and multi-column 
compound B-Tree indexes to high-cardinality foreign keys and date fields.

Integration of EXPLAIN QUERY PLAN command prints in the console log to prove that full table scans 
have been successfully replaced by index searches.

Implementation of SQLite pragmas (PRAGMA journal_mode = WAL;, PRAGMA foreign_keys = ON;) and 
online backup automation using SQLite's backup API
"""