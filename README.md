
# MarTech Engagement → Audience Pipeline

## Overview
End-to-end data pipeline that ingests GitHub events, transforms them into user engagement metrics, and builds marketing audiences with suppression logic.

---

## Objective
Build a lightweight, production-style pipeline demonstrating:
- Data ingestion
- SQL transformations
- Audience segmentation
- Idempotent processing
- Validation logic

---

## Data Source
GitHub Public Events API  
Repo: apache/airflow

Provides recent activity like:
- Pull requests
- Issues
- Push events

Note: API provides a rolling snapshot (not full history).

---

## Tech Stack
- Python
- SQLite
- Pandas
- Requests

---

## How to Run

```bash
pip install -r requirements.txt
python run_pipeline.py

**Pipeline Flow**

GitHub API → raw_events → transformations → user_profile → audiences → CSV outputs

Audience Definitions
High Intent Users

Users with:

events_last_7d ≥ 2
not in suppression list
Suppressed Users

Excluded using suppression_list.csv

Outputs
events.db (SQLite database)
high_intent_users.csv
newly_engaged_users.csv
CLI summary output
Idempotency
event_id is PRIMARY KEY
INSERT OR IGNORE prevents duplicates
safe to re-run pipeline
Validation

Run SQL checks:

sqlite3 events.db
.read queries.sql

Includes:

event trends
top users
audience counts
suppression validation
event type distribution
Key Design Decisions
SQLite for simplicity
SQL-based transformations
API-based ingestion
Lightweight, reproducible pipeline
Focus on clarity over complexity
