# MarTech Engagement → Audience Pipeline

## 📌 Overview

This project is an end-to-end data pipeline that ingests GitHub public events, transforms them into user engagement metrics, and builds activation-ready marketing audiences with suppression logic using Python and SQLite.

It demonstrates practical data engineering skills including:
- API ingestion
- Data modeling
- SQL-based transformations
- Audience segmentation
- Idempotent pipeline design
- Handling real-world data limitations

---

## 🎯 Objective

To build a simple, production-style data pipeline under time constraints, focusing on:
- Correctness of data flow
- Idempotent ingestion
- Clean SQL transformations
- Practical audience generation logic
- Reproducibility and simplicity

---

## 📊 Data Source

The pipeline uses the GitHub Public Events API:

:contentReference[oaicite:0]{index=0}

Repository used:
- apache/airflow

This API provides real-time GitHub activity such as:
- Push events
- Pull requests
- Issue comments
- Repository interactions

---

## ⚙️ Tech Stack

- Python
- SQLite
- Pandas
- Requests
- GitHub REST API

---

## 🚀 How to Run

### Install dependencies
```bash
pip install -r requirements.txt

## Run pipeline
python run_pipeline.py

🧱 Pipeline Flow
Extract events from GitHub API
Load into SQLite (raw_events)
Transform into:
user_daily_engagement
user_profile
Apply suppression list
Build audiences:
High-intent users
Newly engaged users
🗄️ Database
SQLite used for lightweight storage
Auto-created at runtime
Fully portable
📌 Idempotency
event_id used as PRIMARY KEY
INSERT OR IGNORE prevents duplicates
Safe to re-run pipeline
🎯 Audience Definitions
High Intent Users: users with events_last_7d ≥ 2 (tuned for dataset size)
Newly Engaged Users: users with first_seen_ts within last 7 days
Suppressed users are excluded from all audiences
📊 Output

Pipeline prints:

Total events ingested
Distinct users
Audience sizes
Sample users per segment
⚠️ Data Limitations
GitHub API returns a limited streaming dataset
Not full historical coverage
Sparse user activity
High-intent segment may be zero depending on snapshot
🧪 Validation

See queries.sql for:

Event trends
Top users
Audience counts
Suppression validation
Event type distribution
📁 Project Structure
run_pipeline.py
requirements.txt
suppression_list.csv
queries.sql
events.db (auto-generated)
💡 Key Design Decisions
SQLite chosen for simplicity and portability
GitHub API used for real-world event ingestion
SQL used for transformations (no heavy frameworks)
Threshold tuned based on observed data distribution
Focus on simplicity + reproducibility
✅ Summary

Lightweight end-to-end MarTech pipeline that transforms raw GitHub events into actionable user segments using a clean, idempotent, production-style design.
