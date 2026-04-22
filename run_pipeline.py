import requests
import sqlite3
import json
import pandas as pd

DB_PATH = "events.db"
API_URL = "https://api.github.com/repos/apache/airflow/events"


# -----------------------
# CREATE TABLES
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
# FETCH EVENTS
# -----------------------
def fetch_events():
    print("Fetching GitHub events...")
    response = requests.get(API_URL)
    response.raise_for_status()
    return response.json()


# -----------------------
# INSERT EVENTS (IDEMPOTENT)
# -----------------------
def insert_events(conn, events):
    print("Inserting events...")

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

        except Exception as ex:
            print("Skipping event:", ex)

    conn.commit()
    return inserted


# -----------------------
# TRANSFORMATIONS
# -----------------------
def run_transformations(conn):
    print("Running transformations...")

    cursor = conn.cursor()

    cursor.execute("DROP TABLE IF EXISTS user_daily_engagement")
    cursor.execute("""
    CREATE TABLE user_daily_engagement AS
    SELECT 
        user_login,
        DATE(event_ts) as dt,
        COUNT(*) as events_count
    FROM raw_events
    GROUP BY user_login, dt
    """)

    cursor.execute("DROP TABLE IF EXISTS user_profile")
    cursor.execute("""
    CREATE TABLE user_profile AS
    SELECT 
        user_login,
        MIN(event_ts) as first_seen_ts,
        MAX(event_ts) as last_seen_ts,
        COUNT(*) as events_last_7d
    FROM raw_events
    GROUP BY user_login
    """)

    conn.commit()


# -----------------------
# SUPPRESSION LIST
# -----------------------
def load_suppression(conn):
    print("Loading suppression list...")

    df = pd.read_csv("suppression_list.csv")
    df.to_sql("suppression_list", conn, if_exists="replace", index=False)


# -----------------------
# AUDIENCES
# -----------------------
def build_audiences(conn):
    print("Building audiences...")

    cursor = conn.cursor()

    HIGH_INTENT_THRESHOLD = 2

    cursor.execute("DROP TABLE IF EXISTS aud_high_intent_users")
    cursor.execute(f"""
    CREATE TABLE aud_high_intent_users AS
    SELECT 
        u.user_login,
        DATETIME('now') as computed_at,
        u.events_last_7d
    FROM user_profile u
    LEFT JOIN suppression_list s
        ON u.user_login = s.user_login
    WHERE u.events_last_7d >= {HIGH_INTENT_THRESHOLD}
      AND s.user_login IS NULL
    """)

    cursor.execute("DROP TABLE IF EXISTS aud_newly_engaged_users")
    cursor.execute("""
    CREATE TABLE aud_newly_engaged_users AS
    SELECT 
        u.user_login,
        DATETIME('now') as computed_at,
        u.first_seen_ts
    FROM user_profile u
    LEFT JOIN suppression_list s
        ON u.user_login = s.user_login
    WHERE DATE(u.first_seen_ts) >= DATE('now', '-7 day')
      AND s.user_login IS NULL
    """)

    conn.commit()


# -----------------------
# EXPORT OUTPUT FILES (NEW)
# -----------------------
def export_outputs(conn):
    print("Exporting final CSV outputs...")

    df1 = pd.read_sql_query("""
        SELECT * FROM aud_high_intent_users
    """, conn)

    df1.to_csv("high_intent_users.csv", index=False)

    df2 = pd.read_sql_query("""
        SELECT * FROM aud_newly_engaged_users
    """, conn)

    df2.to_csv("newly_engaged_users.csv", index=False)


# -----------------------
# SUMMARY
# -----------------------
def print_summary(conn, inserted):
    cursor = conn.cursor()

    total_events = cursor.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0]
    distinct_users = cursor.execute("SELECT COUNT(DISTINCT user_login) FROM raw_events").fetchone()[0]
    high_intent = cursor.execute("SELECT COUNT(*) FROM aud_high_intent_users").fetchone()[0]
    new_users = cursor.execute("SELECT COUNT(*) FROM aud_newly_engaged_users").fetchone()[0]

    print("\n--- PIPELINE SUMMARY ---")
    print(f"New events inserted: {inserted}")
    print(f"Total events: {total_events}")
    print(f"Distinct users: {distinct_users}")
    print(f"High intent users: {high_intent}")
    print(f"New users: {new_users}")

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

    export_outputs(conn)  # ✅ NEW STEP ADDED

    print_summary(conn, inserted)

    conn.close()


if __name__ == "__main__":
    main()
