# MarTech Engagement → Audience Pipeline

## Overview
This project is an end-to-end data pipeline that ingests GitHub public events, transforms them into user engagement metrics, and builds marketing-ready audiences using suppression logic.

It demonstrates practical data engineering skills including ingestion, transformation, SQL-based modeling, and validation.

## Objective
Build a lightweight pipeline that converts raw GitHub events into actionable user audiences using SQL-based transformations and simple business rules.

## Data Source
GitHub Public Events API  
Repository: apache/airflow  

Provides recent activity such as:
- Pull requests
- Issues
- Push events

Note: This is a rolling, rate-limited snapshot of recent events (not full historical data), so results may vary depending on API activity.
---

## Tech Stack
- Python
- SQLite
- Pandas
- Requests
- 
## How to Run

(```bash)
pip install -r requirements.txt
python run_pipeline.py

## Pipeline Flow
GitHub API → raw_events → user_profile → audiences → CSV outputs

## Audience Definitions

# High Intent Users
Users with:
events_last_7d ≥ 2
Not in suppression list

# Newly Engaged Users
Users whose first event is within last 7 days
(excluding suppressed users)

# Suppression Logic
Users listed in suppression_list.csv are excluded from all audiences.

## Outputs
- events.db (SQLite database containing raw and transformed data)
- high_intent_users.csv (users with high engagement activity)
- newly_engaged_users.csv (recently active users)
- CLI summary output (pipeline execution metrics and validation results)

## Idempotency
event_id is PRIMARY KEY
INSERT OR IGNORE prevents duplicates
Pipeline is safe to re-run without duplication

## Validation

Run the following to validate:

sqlite3 events.db
.read queries.sql

# Includes:
Event trends
Top users
Audience counts
Suppression validation
Event type distribution

## Key Design Decisions
SQLite used for simplicity and portability
SQL used for transformations (no heavy frameworks)
API-based ingestion for realism
Lightweight, reproducible pipeline
Focus on correctness over complexity

# Summary

A lightweight end-to-end data pipeline that converts raw GitHub events into actionable marketing audiences using clean, reproducible data engineering practices.
