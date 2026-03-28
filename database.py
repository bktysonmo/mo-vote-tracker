import sqlite3  # sqlite3 is built into Python — no install needed

def get_connection():
    conn = sqlite3.connect("mo_votes.db")
    conn.row_factory = sqlite3.Row
    return conn

def setup_database():
    conn = get_connection()
    cursor = conn.cursor()

    # TABLE 1: legislators
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS legislators (
            people_id   INTEGER PRIMARY KEY,
            name        TEXT,
            party       TEXT,
            role        TEXT,
            district    TEXT,
            chamber     TEXT
        )
    """)

    # TABLE 2: bills
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bills (
            bill_id     INTEGER PRIMARY KEY,
            bill_number TEXT,
            title       TEXT,
            session     TEXT,
            url         TEXT,
            status      TEXT,   -- e.g. "Introduced", "Passed", "Failed", "Vetoed"
            chamber     TEXT    -- originating chamber: "House" or "Senate"
        )
    """)

    # TABLE 3: votes (roll call events)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            roll_call_id  INTEGER PRIMARY KEY,
            bill_id       INTEGER,
            date          TEXT,
            description   TEXT,
            yea           INTEGER,
            nay           INTEGER,
            nv            INTEGER,
            passed        INTEGER,
            chamber       TEXT,
            FOREIGN KEY (bill_id) REFERENCES bills(bill_id)
        )
    """)

    # TABLE 4: member_votes
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS member_votes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            roll_call_id  INTEGER,
            people_id     INTEGER,
            vote_text     TEXT,
            FOREIGN KEY (roll_call_id) REFERENCES votes(roll_call_id),
            FOREIGN KEY (people_id) REFERENCES legislators(people_id),
            UNIQUE(roll_call_id, people_id)
        )
    """)

    # TABLE 5: sponsors
    # One row per sponsor per bill — a bill can have multiple sponsors
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sponsors (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id     INTEGER,  -- Which bill
            people_id   INTEGER,  -- Which legislator (matches legislators.people_id)
            name        TEXT,     -- Stored directly in case legislator isn't in our table
            sponsor_type TEXT,    -- "Primary" or "Co-Sponsor"
            FOREIGN KEY (bill_id) REFERENCES bills(bill_id),
            UNIQUE(bill_id, people_id)  -- No duplicate sponsor entries per bill
        )
    """)

    # TABLE 6: committees
    # One row per committee referral per bill
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS committees (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id         INTEGER,  -- Which bill
            committee_name  TEXT,     -- e.g. "Ways and Means"
            chamber         TEXT,     -- "House" or "Senate"
            FOREIGN KEY (bill_id) REFERENCES bills(bill_id),
            UNIQUE(bill_id, committee_name)
        )
    """)

    conn.commit()
    conn.close()
    print("Database ready.")

if __name__ == "__main__":
    setup_database()