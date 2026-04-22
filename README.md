# MarTech Engagement → Audience Pipeline

## Overview
This project is an end-to-end data pipeline that ingests GitHub public events, transforms them into user engagement metrics, and builds marketing-ready audiences using suppression logic.

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

## Tech Stack
- Python
- SQLite
- Pandas
- Requests

## How to Run
(```bash)
1. pip install -r requirements.txt
2. python run_pipeline.py

## Pipeline Flow
GitHub API → raw_events → user_profile → audiences → CSV outputs

## Audience Definitions

#### High Intent Users
Users with:
- events_last_7d ≥ 2  
- Not present in the suppression list

#### Newly Engaged Users
Users whose first event occurred within the last 7 days (excluding suppressed users).

#### Suppression Logic
Users listed in `suppression_list.csv` are excluded from all audiences.

## Outputs
- events.db (SQLite database containing raw and transformed data)
- high_intent_users.csv (users with high engagement activity)
- newly_engaged_users.csv (recently active users)  
- CLI summary output (sample run)
  
    - New events inserted: 10
    - Total events: 54
    - Distinct users: 21
    - High intent users: 10
    - New users: 19
  
## Idempotency
- event_id is PRIMARY KEY
- INSERT OR IGNORE prevents duplicates
- Pipeline is safe to re-run without duplication

## Validation
Run the following to validate:
- sqlite3 events.db
 .read queries.sql

#### Includes queries for:
- Event trends over time  
- Top active users  
- Audience size validation  
- Suppression check (should return 0 results)  
- Event type distribution  

## Key Design Decisions
- SQL used for all transformations for simplicity and clarity  
- GitHub API used as the data source for ingestion  
- Design kept lightweight and reproducible using SQLite  

# Summary
This project implements an end-to-end data pipeline that ingests GitHub events, processes them into structured user metrics, and generates marketing-ready audiences with suppression and validation logic.
