### Lab Description: 
Create a dedicated downstream Data Mart table by extracting and aggregating relevant data 
from your core warehouse schema using Python or SQL. Implement performance optimization techniques by 
adding appropriate indexes (CREATE INDEX) on frequently filtered columns. Finally, document the schema in a 
concise data dictionary detailing column names, data types, and business definitions. Provide the complete 
DDL script and data dictionary documentation as the solution.

### Lab Objective:
You will optimize database read performance by implementing structural tuning techniques (indexing) on their existing 
SQLite data warehouse and isolate business intelligence workloads by designing, extracting, and materializing a 
dedicated Departmental Data Mart (e.g., a Sales & Revenue Data Mart) using Python.

### Data Mart Extraction:
Extract targeted departmental 
marts isolating marketing metrics, customer churn indicators, property valuations, or viewer watch-time 
summaries.

### Step-by-Step Instructions:
1. Locate Your Group Database: Open your group's repository workspace and locate your core SQLite database 
file (e.g., ecommerce_warehouse.db, supply_chain_warehouse.db, etc.).
2. Analyze Performance Bottlenecks: Identify which columns your group queried most frequently during your 
OLAP and window function exercises (e.g., foreign keys, dates, category/status fields).
3. Apply Custom Indexes: Write and execute explicit SQL CREATE INDEX statements tailored to your group's 
schema to eliminate full table scans.
4. Materialize Your Departmental Data Mart: Write a Python script using pandas and sqlite3 that queries your 
tuned core warehouse, performs a departmental aggregation, and saves the result into a dedicated secondary 
database (e.g., department_datamart.db).
5. Submit Execution Proof: Save your execution logs showing row counts, indexing statements, and query speed 
comparisons to MS Teams alongside your final group report.

### Database Schema Blueprints:
You must implement a star schema separating operational transactional metrics into a central Fact table surrounded 
by descriptive Dimension tables.

* Dimension Tables (dim_customer, dim_product, dim_store)
  * dim_customer
    * customer_id (TEXT, Primary Key)
    * customer_name (TEXT)
    * customer_segment (TEXT - e.g., Retail, Corporate)
    * signup_date (DATETIME)
  * dim_product
    * product_id (TEXT, Primary Key)
    * product_name (TEXT)
    * product_category (TEXT)
    * standard_price (REAL)
  * dim_store (or Location)
    * store_id (TEXT, Primary Key)
    * store_name (TEXT)
    * region (TEXT)
    * city (TEXT)
* Central Fact Table (fact_sales)
  * transaction_id (TEXT, Primary Key)
  * customer_id (TEXT, Foreign Key referencing dim_customer(customer_id))
  * product_id (TEXT, Foreign Key referencing dim_product(product_id))
  * store_id (TEXT, Foreign Key referencing dim_store(store_id))
  * quantity (INTEGER)
  * unit_price (REAL)
  * shipping_cost (REAL)
  * order_date (DATETIME)
  * status (TEXT - e.g., Completed, Pending, Cancelled)

### Document Store Schema Blueprint (MongoDB)
For the non-relational component, you must design flexible collections in MongoDB that capture semi-structured 
attributes, logs, or feedback.

* Collection: sales_documents
  * Stores fully denormalized or semi-structured transaction records for fast analytical grouping.
* Collection: feedback_documents (or Sensor/Audit Logs)
  * Stores unstructured text comments, ratings, and telemetry payloads.

### Domain-Specific Schema Adjustments 
Modify fact table metrics (e.g., replacing unit_price with treatment_cost, transaction_amount, watch_time_minutes, or property_price).

### Rubrics Based Requirements 
* Data mart is cleanly isolated and tailored precisely to departmental analytical needs.
* Indexes are strategically placed on high-cardinality join and filter columns, measurably improving query performance.
* Comprehensive, clear data dictionary provided with accurate technical and business definitions.