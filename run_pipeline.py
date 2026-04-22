import requests
import sqlite3
import json
import pandas as pd

DB_PATH = "events.db"
API_URL = "https://api.github.com/repos/apache/airflow/events"
THRESHOLD = 2


def fetch_events():
    print("Fetching GitHub events...")

    headers = {
        "User-Agent": "pipeline",
        "Accept": "application/vnd.github+json"
    }

    try:
        r = requests.get(API_URL, headers=headers, timeout=10)

        if r.status_code == 403:
            print("Rate limit hit → empty dataset")
            return []

        return r.json()

    except Exception as e:
        print("API error:", e)
        return []


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


def load_events(conn, events):
    cur = conn.cursor()
    inserted = 0

    for e in events:
        cur.execute("""
        INSERT OR IGNORE INTO raw_events VALUES (?, ?, ?, ?, ?, ?)
        """, (
            e.get("id"),
            e.get("created_at"),
            e.get("actor", {}).get("login"),
            e.get("repo", {}).get("name"),
            e.get("type"),
            json.dumps(e.get("payload"))
        ))

        if cur.rowcount > 0:
            inserted += 1

    conn.commit()
    return inserted


def transform(conn):
    conn.execute("DROP TABLE IF EXISTS user_profile")

    conn.execute("""
    CREATE TABLE user_profile AS
    SELECT
        user_login,
        MIN(event_ts) AS first_seen,
        MAX(event_ts) AS last_seen,
        COUNT(*) AS events_last_7d
    FROM raw_events
    GROUP BY user_login
    """)


def suppression(conn):
    df = pd.read_csv("suppression_list.csv")
    df.to_sql("suppression_list", conn, if_exists="replace", index=False)


def audiences(conn):
    conn.execute("DROP TABLE IF EXISTS high_intent")

    conn.execute(f"""
    CREATE TABLE high_intent AS
    SELECT u.user_login, u.events_last_7d
    FROM user_profile u
    LEFT JOIN suppression_list s
    ON u.user_login = s.user_login
    WHERE u.events_last_7d >= {THRESHOLD}
    AND s.user_login IS NULL
    """)


def summary(conn, inserted):
    cur = conn.cursor()

    print("\n--- SUMMARY ---")
    print("Inserted:", inserted)
    print("Users:", cur.execute("SELECT COUNT(DISTINCT user_login) FROM raw_events").fetchone()[0])
    print("High intent:", cur.execute("SELECT COUNT(*) FROM high_intent").fetchone()[0])


def main():
    conn = sqlite3.connect(DB_PATH)

    create_tables(conn)

    events = fetch_events()
    inserted = load_events(conn, events)

    transform(conn)
    suppression(conn)
    audiences(conn)

    summary(conn, inserted)

    conn.close()


if __name__ == "__main__":
    main()
