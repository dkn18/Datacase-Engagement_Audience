# Datacase-Engagement_Audience
End-to-end data pipeline that ingests GitHub public events, transforms them into user engagement metrics, and builds activation-ready marketing audiences with suppression logic using Python and SQLite.

Goal :
To demonstrate practical data engineering skills in building a simple, production-style pipeline under time constraints, focusing on correctness, idempotency, and clean design.

# MarTech Audience Pipeline

## Overview
This project builds a simple end-to-end data pipeline that converts engagement events into activation-ready audiences.

It:
- Fetches GitHub events
- Stores them in SQLite
- Transforms data into analytics tables
- Applies suppression rules
- Generates marketing audiences

---

## Data Source
Uses the GitHub Public Events API for:
apache/airflow

This provides real-time engagement data like pushes, pull requests, and comments.

---

## Setup

```bash
pip install -r requirements.txt


Note : SQLite is used as a lightweight embedded database. The database file is created automatically at runtime, ensuring portability and zero setup overhead.

## Data Note

The GitHub Public Events API returns a limited number of recent events.
Because of this, audience sizes may vary depending on repository activity at runtime.

To ensure meaningful results for small datasets, the high-intent threshold has been tuned to a lower value.
