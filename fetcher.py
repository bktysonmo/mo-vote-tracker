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
    })
    data = response.json()
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
    print(f"Found {len(bill_entries)} bills. Fetching vote data for each...")
    conn = get_connection()
    cursor = conn.cursor()
    for i, bill_summary in enumerate(bill_entries):
        bill_id = bill_summary.get("bill_id")
        if not bill_id:
            continue
        try:
            bill_data = legiscan_call("getBill", id=bill_id)
            bill = bill_data.get("bill", {})
            cursor.execute("""
                INSERT OR REPLACE INTO bills (bill_id, bill_number, title, session, url)
                VALUES (?, ?, ?, ?, ?)
            """, (
                bill.get("bill_id"),
                bill.get("bill_number", ""),
                bill.get("title", ""),
                bill.get("session", {}).get("session_name", ""),
                bill.get("state_link", "")
            ))
            for vote_summary in bill.get("votes", []):
                roll_call_id = vote_summary.get("roll_call_id")
                if not roll_call_id:
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
            time.sleep(0.2)
            if i % 10 == 0:
                print(f"  Progress: {i}/{len(bill_entries)} bills...")
        except Exception as e:
            print(f"  Skipping bill {bill_id}: {e}")
            continue
    conn.close()
    print("Done fetching votes.")

def run_full_fetch():
    print("=== Missouri Vote Tracker: Starting data fetch ===")
    setup_database()
    session_id = get_current_session_id()
    fetch_and_store_legislators(session_id)
    fetch_and_store_votes(session_id)
    print("=== Fetch complete. Your database is ready. ===")

if __name__ == "__main__":
    run_full_fetch()