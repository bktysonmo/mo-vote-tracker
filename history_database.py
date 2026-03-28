import sqlite3

HISTORY_DB = "mo_history.db"

def get_history_connection():
    conn = sqlite3.connect(HISTORY_DB)
    conn.row_factory = sqlite3.Row
    return conn

def setup_history_database():
    conn = get_history_connection()
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            session_id    INTEGER PRIMARY KEY,
            session_name  TEXT,
            year          INTEGER,
            special       INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS legislators (
            people_id         INTEGER,
            session_id        INTEGER,
            name              TEXT,
            party             TEXT,
            role              TEXT,
            district          TEXT,
            chamber           TEXT,
            currently_serving INTEGER DEFAULT 0,
            PRIMARY KEY (people_id, session_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bills (
            bill_id       INTEGER,
            session_id    INTEGER,
            session_name  TEXT,
            bill_number   TEXT,
            title         TEXT,
            status        TEXT,
            chamber       TEXT,
            url           TEXT,
            PRIMARY KEY (bill_id, session_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS similar_bills (
            bill_id         INTEGER,
            similar_bill_id INTEGER,
            similar_number  TEXT,
            similar_title   TEXT,
            similar_session TEXT,
            PRIMARY KEY (bill_id, similar_bill_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            roll_call_id  INTEGER PRIMARY KEY,
            bill_id       INTEGER,
            session_id    INTEGER,
            date          TEXT,
            description   TEXT,
            yea           INTEGER,
            nay           INTEGER,
            nv            INTEGER,
            passed        INTEGER,
            chamber       TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS member_votes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            roll_call_id  INTEGER,
            people_id     INTEGER,
            session_id    INTEGER,
            vote_text     TEXT,
            UNIQUE(roll_call_id, people_id)
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sponsors (
            bill_id       INTEGER,
            session_id    INTEGER,
            name          TEXT,
            sponsor_type  TEXT,
            people_id     INTEGER
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS committees (
            bill_id         INTEGER,
            session_id      INTEGER,
            committee_name  TEXT,
            chamber         TEXT
        )
    """)

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fetch_progress (
            session_id  INTEGER PRIMARY KEY,
            completed   INTEGER DEFAULT 0
        )
    """)

    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mv_people ON member_votes(people_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_mv_rollcall ON member_votes(roll_call_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_votes_bill ON votes(bill_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_bills_session ON bills(session_id)")
    cursor.execute("CREATE INDEX IF NOT EXISTS idx_legislators_people ON legislators(people_id)")

    conn.commit()
    conn.close()
    print("History database ready.")

if __name__ == "__main__":
    setup_history_database()