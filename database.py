import sqlite3  # sqlite3 is built into Python — no install needed
                # It lets us create and talk to our database file

def get_connection():
    # This opens a connection to our database file
    # If the file doesn't exist yet, SQLite creates it automatically
    conn = sqlite3.connect("mo_votes.db")
    conn.row_factory = sqlite3.Row  # This makes results behave like dictionaries
                                    # so we can say result["name"] instead of result[0]
    return conn

def setup_database():
    # This function creates our tables if they don't already exist
    # Think of tables like individual spreadsheet tabs inside one file
    conn = get_connection()
    cursor = conn.cursor()  # A cursor is like a pen — we use it to write instructions to the DB

    # TABLE 1: legislators
    # Stores every Missouri House and Senate member
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS legislators (
            people_id   INTEGER PRIMARY KEY,  -- LegiScan's unique ID for each person
            name        TEXT,                 -- Full name
            party       TEXT,                 -- R, D, I, etc.
            role        TEXT,                 -- "Rep" or "Sen"
            district    TEXT,                 -- District number
            chamber     TEXT                  -- "House" or "Senate"
        )
    """)

    # TABLE 2: bills
    # Stores every bill that has come up for a vote
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS bills (
            bill_id     INTEGER PRIMARY KEY,  -- LegiScan's unique ID for each bill
            bill_number TEXT,                 -- e.g. "HB 42" or "SB 17"
            title       TEXT,                 -- The bill's short description
            session     TEXT,                 -- e.g. "2025 Regular Session"
            url         TEXT                  -- Link to the bill on legiscan.com
        )
    """)

    # TABLE 3: votes (the roll call events themselves)
    # Each row is one roll call vote — a moment when members voted on something
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS votes (
            roll_call_id  INTEGER PRIMARY KEY,  -- LegiScan's unique ID for each roll call
            bill_id       INTEGER,              -- Which bill was being voted on
            date          TEXT,                 -- Date of the vote
            description   TEXT,                 -- What was being voted on (e.g. "Third Reading")
            yea           INTEGER,              -- Total yes votes
            nay           INTEGER,              -- Total no votes
            nv            INTEGER,              -- Total not voting
            passed        INTEGER,              -- 1 = passed, 0 = failed
            chamber       TEXT,                 -- "House" or "Senate"
            FOREIGN KEY (bill_id) REFERENCES bills(bill_id)
            -- FOREIGN KEY means: the bill_id here must exist in the bills table
            -- This keeps your data consistent
        )
    """)

    # TABLE 4: member_votes
    # This is the most important table — how each individual legislator voted on each roll call
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS member_votes (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,  -- We create our own ID here
            roll_call_id  INTEGER,   -- Which roll call vote
            people_id     INTEGER,   -- Which legislator
            vote_text     TEXT,      -- "Yea", "Nay", "NV" (not voting), "Absent"
            FOREIGN KEY (roll_call_id) REFERENCES votes(roll_call_id),
            FOREIGN KEY (people_id) REFERENCES legislators(people_id),
            UNIQUE(roll_call_id, people_id)  -- Prevents storing the same person's vote twice
        )
    """)

    conn.commit()   # commit() = save all the changes, like hitting Ctrl+S
    conn.close()    # Always close the connection when done
    print("Database ready.")

# This block means: only run setup_database() if we run THIS file directly
# If another file imports database.py, this won't auto-run
if __name__ == "__main__":
    setup_database()