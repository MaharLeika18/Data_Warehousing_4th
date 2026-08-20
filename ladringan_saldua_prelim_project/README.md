# Prelim Mini Project
### Project 7: E-Commerce Real-Time Inventory & Abandoned Cart Analytics
An end-to-end, production-grade, fault-tolerant data pipeline that ingests data from a simulated transactional database, implements schema validation and cryptographic PII data masking, isolates operational errors, and writes optimized, analytical outputs to a simulated data lake.

The data workflow eliminates duplicate session actions, quarantines rows with missing identifiers, computes total abandoned product metrics, and constructs a high-performance Hive-partitioned directory layout structured by product department.


![Pipeline Diagram](https://github.com/MaharLeika18/Data_Warehousing_4th/blob/c7fa279017dd1c9619cfbdf400da762055038348/ladringan_saldua_prelim_project/img/Diagram.png)


## Setup

1. Install dependencies:
   ```pip install -r requirements.txt```

2. Generate your own PII salt (do not reuse someone else's):
   ```python -c "import secrets; print(secrets.token_hex(32))"```

3. Copy the env template and paste your salt into it:
   ```cp .env.example .env```
   then edit .env and set ```PII_SALT=<the string you generated>```

4. Edit config.py's ``get_config()`` function to point ``SQLITE_DB_PATH`` and
   ``SOURCE_TABLE`` at your own dataset, and update ``SOURCE_SCHEMA`` to match
   your table's actual columns.

5. Run the pipeline:
   ```python ladringan_saldua_prelim_project/cart_analytics_orchestrator.py```
