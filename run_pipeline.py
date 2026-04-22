import requests
import sqlite3
import json
import pandas as pd
import time

DB_PATH = "events.db"
API_URL = "https://api.github.com/repos/apache/airflow/events"


# -----------------------
# TABLES
# -----------------------
def create_tables(conn):
    conn.execute("""
    CREATE TABLE IF NOT EXISTS raw_events (
        event_id TEXT PRIMARY KEY,
        event_ts TEXT,
        user_login TEXT,
        repo TEXT,
        event_type TEXT,
        payload_json TEXT
    )
    """)


# -----------------------
# FETCH EVENTS (ROBUST)
# -----------------------
def fetch_events():
    print("Fetching GitHub events...")

    headers = {
        "User-Agent": "martech-pipeline",
        "Accept": "application/vnd.github+json"
    }

    max_retries = 3
    wait_time = 5

    for attempt in range(max_retries):
        try:
            response = requests.get(API_URL, headers=headers, timeout=10)

            if response.status_code == 403:
                print(f"Rate limit hit. Retry {attempt+1}/{max_retries}")
                time.sleep(wait_time)
                wait_time *= 2
                continue

            response.raise_for_status()
            return response.json()

        except requests.exceptions.RequestException as e:
            print(f"API error: {e}. Retry {attempt+1}/{max_retries}")
            time.sleep(wait_time)
            wait_time *= 2

    print("API failed after retries. Using empty dataset fallback.")
    return []


# -----------------------
# INSERT EVENTS
# -----------------------
def insert_events(conn, events):
    cursor = conn.cursor()
    inserted = 0

    for e in events:
        try:
            cursor.execute("""
            INSERT OR IGNORE INTO raw_events 
            (event_id, event_ts, user_login, repo, event_type, payload_json)
            VALUES (?, ?, ?, ?, ?, ?)
            """, (
                e.get("id"),
                e.get("created_at"),
                e.get("actor", {}).get("login"),
                e.get("repo", {}).get("name"),
                e.get("type"),
                json.dumps(e.get("payload"))
            ))

            if cursor.rowcount > 0:
                inserted += 1

        except Exception:
            continue

    conn.commit()
    return inserted


# -----------------------
# TRANSFORM
# -----------------------
def run_transformations(conn):
    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS user_daily_engagement")
    cursor.execute("""
    CREATE TABLE user_daily_engagement AS
    SELECT user_login, DATE(event_ts) dt, COUNT(*) events_count
    FROM raw_events
    GROUP BY user_login, DATE(event_ts)
    """)

    cursor.execute("DROP TABLE IF EXISTS user_profile")
    cursor.execute("""
    CREATE TABLE user_profile AS
    SELECT 
        user_login,
        MIN(event_ts) first_seen_ts,
        MAX(event_ts) last_seen_ts,
        COUNT(*) events_last_7d
    FROM raw_events
    GROUP BY user_login
    """)

    conn.commit()


# -----------------------
# SUPPRESSION
# -----------------------
def load_suppression(conn):
    df = pd.read_csv("suppression_list.csv")
    df.to_sql("suppression_list", conn, if_exists="replace", index=False)


# -----------------------
# AUDIENCES
# -----------------------
def build_audiences(conn):
    cursor = conn.cursor()

    HIGH_INTENT_THRESHOLD = 2

    cursor.execute("DROP TABLE IF EXISTS aud_high_intent_users")
    cursor.execute(f"""
    CREATE TABLE aud_high_intent_users AS
    SELECT u.user_login, DATETIME('now') computed_at, u.events_last_7d
    FROM user_profile u
    LEFT JOIN suppression_list s
        ON u.user_login = s.user_login
    WHERE u.events_last_7d >= {HIGH_INTENT_THRESHOLD}
      AND s.user_login IS NULL
    """)

    cursor.execute("DROP TABLE IF EXISTS aud_newly_engaged_users")
    cursor.execute("""
    CREATE TABLE aud_newly_engaged_users AS
    SELECT u.user_login, DATETIME('now') computed_at, u.first_seen_ts
    FROM user_profile u
    LEFT JOIN suppression_list s
        ON u.user_login = s.user_login
    WHERE DATE(u.first_seen_ts) >= DATE('now', '-7 day')
      AND s.user_login IS NULL
    """)

    conn.commit()


# -----------------------
# EXPORT OUTPUTS (CSV LAYER)
# -----------------------
def export_outputs(conn):
    print("Exporting CSV outputs...")

    pd.read_sql("SELECT * FROM aud_high_intent_users", conn)\
        .to_csv("high_intent_users.csv", index=False)

    pd.read_sql("SELECT * FROM aud_newly_engaged_users", conn)\
        .to_csv("newly_engaged_users.csv", index=False)


# -----------------------
# SUMMARY
# -----------------------
def print_summary(conn, inserted):
    cursor = conn.cursor()

    print("\n--- PIPELINE SUMMARY ---")
    print(f"New events inserted: {inserted}")
    print(f"Total events: {cursor.execute('SELECT COUNT(*) FROM raw_events').fetchone()[0]}")
    print(f"Distinct users: {cursor.execute('SELECT COUNT(DISTINCT user_login) FROM raw_events').fetchone()[0]}")
    print(f"High intent users: {cursor.execute('SELECT COUNT(*) FROM aud_high_intent_users').fetchone()[0]}")
    print(f"New users: {cursor.execute('SELECT COUNT(*) FROM aud_newly_engaged_users').fetchone()[0]}")

    print("\nSample High Intent Users:")
    for row in cursor.execute("SELECT user_login FROM aud_high_intent_users LIMIT 10"):
        print("-", row[0])

    print("\nSample Newly Engaged Users:")
    for row in cursor.execute("SELECT user_login FROM aud_newly_engaged_users LIMIT 10"):
        print("-", row[0])


# -----------------------
# MAIN
# -----------------------
def main():
    conn = sqlite3.connect(DB_PATH)

    create_tables(conn)

    events = fetch_events()
    inserted = insert_events(conn, events)

    run_transformations(conn)
    load_suppression(conn)
    build_audiences(conn)

    export_outputs(conn)

    print_summary(conn, inserted)

    conn.close()


if __name__ == "__main__":
    main()
