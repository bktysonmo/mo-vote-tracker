import os
import time
import requests
from dotenv import load_dotenv
from database import get_connection, setup_database

load_dotenv()

API_KEY = os.getenv("LEGISCAN_API_KEY")
BASE_URL = "https://api.legiscan.com/"
MO_STATE = "MO"

def legiscan_call(op, **params):
    response = requests.get(BASE_URL, params={
        "key": API_KEY,
        "op": op,
        **params
    }, timeout=30)
    try:
        data = response.json()
    except Exception:
        raise Exception(f"LegiScan returned non-JSON response for {op}")
    if data.get("status") == "ERROR":
        raise Exception(f"LegiScan API error on {op}: {data.get('alert', 'unknown error')}")
    return data

def get_current_session_id():
    print("Looking up current Missouri session...")
    data = legiscan_call("getSessionList", state=MO_STATE)
    sessions = data.get("sessions", [])
    for session in sessions:
        if session.get("special", 0) == 0:
            name = session["session_name"]
            sid = session["session_id"]
            print(f"Found session: {name} (ID: {sid})")
            return sid
    print("Falling back to known session ID 2239")
    return 2239

def get_stored_hashes(conn):
    # Returns a dict of {bill_id: change_hash} for all bills we already have
    # change_hash is a fingerprint LegiScan generates whenever a bill changes
    # If the hash matches what we have stored, the bill hasn't changed — skip it
    cursor = conn.cursor()
    cursor.execute("SELECT bill_id, change_hash FROM bills WHERE change_hash IS NOT NULL")
    return {row["bill_id"]: row["change_hash"] for row in cursor.fetchall()}

def ensure_change_hash_column(conn):
    # Add change_hash column to bills table if it doesn't exist yet
    cursor = conn.cursor()
    try:
        cursor.execute("ALTER TABLE bills ADD COLUMN change_hash TEXT")
        conn.commit()
        print("Added change_hash column to bills table.")
    except Exception:
        pass  # Column already exists

def fetch_and_store_legislators(session_id):
    print("Fetching legislators...")
    data = legiscan_call("getSessionPeople", id=session_id)
    people = data.get("sessionpeople", {}).get("people", [])

    conn = get_connection()
    cursor = conn.cursor()
    for person in people:
        role = person.get("role", "")
        chamber = "House" if role == "Rep" else "Senate"
        cursor.execute("""
            INSERT OR REPLACE INTO legislators
                (people_id, name, party, role, district, chamber)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            person["people_id"],
            person["name"],
            person.get("party", ""),
            role,
            str(person.get("district", "")),
            chamber
        ))
    conn.commit()
    conn.close()
    print(f"Stored {len(people)} legislators.")

def fetch_and_store_votes(session_id):
    print("Fetching master bill list...")
    data = legiscan_call("getMasterList", id=session_id)
    masterlist = data.get("masterlist", {})
    bill_entries = [v for k, v in masterlist.items() if k != "0"]
    print(f"Found {len(bill_entries)} bills in session.")

    conn = get_connection()
    ensure_change_hash_column(conn)
    stored_hashes = get_stored_hashes(conn)
    cursor = conn.cursor()

    new_count = 0
    changed_count = 0
    skipped_count = 0
    error_count = 0

    for i, bill_summary in enumerate(bill_entries):
        bill_id = bill_summary.get("bill_id")
        if not bill_id:
            continue

        # Check if bill has changed since we last fetched it
        incoming_hash = bill_summary.get("change_hash", "")
        stored_hash = stored_hashes.get(bill_id)

        if stored_hash and stored_hash == incoming_hash:
            # Hash matches — bill hasn't changed, skip it entirely
            skipped_count += 1
            continue

        # Hash is new or changed — fetch the full bill
        is_new = stored_hash is None
        if is_new:
            new_count += 1
        else:
            changed_count += 1

        try:
            bill_data = legiscan_call("getBill", id=bill_id)
            bill = bill_data.get("bill", {})

            status_map = {1: "Introduced", 2: "Engrossed", 3: "Enrolled",
                          4: "Passed", 5: "Vetoed", 6: "Failed"}
            status_num = bill.get("status", 1)
            status_text = status_map.get(status_num, "Introduced")
            body_id = bill.get("body_id", 0)
            chamber_text = "House" if body_id == 1 else "Senate" if body_id == 2 else ""

            cursor.execute("""
                INSERT OR REPLACE INTO bills
                    (bill_id, bill_number, title, session, url, status, chamber, change_hash)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                bill.get("bill_id"),
                bill.get("bill_number", ""),
                bill.get("title", ""),
                bill.get("session", {}).get("session_name", ""),
                bill.get("state_link", ""),
                status_text,
                chamber_text,
                incoming_hash
            ))

            # Store sponsors
            for sponsor in bill.get("sponsors", []):
                stype = "Primary" if sponsor.get("sponsor_type_id") == 1 else "Co-Sponsor"
                cursor.execute("""
                    INSERT OR IGNORE INTO sponsors
                        (bill_id, name, sponsor_type)
                    VALUES (?, ?, ?)
                """, (bill.get("bill_id"), sponsor.get("name", ""), stype))

            # Store committee
            committee_raw = bill.get("committee", {})
            if isinstance(committee_raw, list):
                committee = committee_raw[0] if committee_raw else {}
            else:
                committee = committee_raw or {}
            committee_name = committee.get("name", "").strip()
            if committee_name:
                cursor.execute("""
                    INSERT OR IGNORE INTO committees
                        (bill_id, committee_name, chamber)
                    VALUES (?, ?, ?)
                """, (bill.get("bill_id"), committee_name, chamber_text))

            # Store roll calls and member votes
            for vote_summary in bill.get("votes", []):
                roll_call_id = vote_summary.get("roll_call_id")
                if not roll_call_id:
                    continue

                # Check if we already have this roll call
                cursor.execute(
                    "SELECT 1 FROM votes WHERE roll_call_id = ?", (roll_call_id,)
                )
                if cursor.fetchone():
                    # Already have this roll call — skip the getRollCall query
                    continue

                rc_data = legiscan_call("getRollCall", id=roll_call_id)
                rc = rc_data.get("roll_call", {})

                cursor.execute("""
                    INSERT OR REPLACE INTO votes
                        (roll_call_id, bill_id, date, description, yea, nay, nv, passed, chamber)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    rc.get("roll_call_id"),
                    bill.get("bill_id"),
                    rc.get("date", ""),
                    rc.get("desc", ""),
                    rc.get("yea", 0),
                    rc.get("nay", 0),
                    rc.get("nv", 0),
                    rc.get("passed", 0),
                    rc.get("chamber", "")
                ))

                for member_vote in rc.get("votes", []):
                    cursor.execute("""
                        INSERT OR IGNORE INTO member_votes
                            (roll_call_id, people_id, vote_text)
                        VALUES (?, ?, ?)
                    """, (
                        rc.get("roll_call_id"),
                        member_vote.get("people_id"),
                        member_vote.get("vote_text", "")
                    ))

                time.sleep(0.3)

            conn.commit()
            time.sleep(0.1)

            if (i + 1) % 50 == 0:
                print(f"  Progress: {i+1}/{len(bill_entries)} | New: {new_count} | Changed: {changed_count} | Skipped: {skipped_count}")

        except Exception as e:
            print(f"  Skipping bill {bill_id}: {e}")
            error_count += 1
            continue

    conn.close()
    print(f"\nFetch complete.")
    print(f"  New bills: {new_count}")
    print(f"  Changed bills: {changed_count}")
    print(f"  Unchanged (skipped): {skipped_count}")
    print(f"  Errors: {error_count}")
    print(f"  API queries used this run: ~{new_count + changed_count + (new_count + changed_count)}")

def run_full_fetch():
    print("=== Missouri Vote Tracker: Starting data fetch ===")
    setup_database()
    session_id = get_current_session_id()
    fetch_and_store_legislators(session_id)
    fetch_and_store_votes(session_id)
    print("=== Fetch complete. Your database is ready. ===")

if __name__ == "__main__":
    run_full_fetch()