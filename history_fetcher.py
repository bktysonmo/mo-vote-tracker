import os
import io
import json
import time
import zipfile
import base64
import requests
from dotenv import load_dotenv
from history_database import get_history_connection, setup_history_database

load_dotenv()

API_KEY = os.getenv("LEGISCAN_API_KEY")
BASE_URL = "https://api.legiscan.com/"

ALL_SESSIONS = [
    (2239, "2026 Regular Session", 2026, 0, "6UzBHz4T6KinkHtOIQfrUK"),
    (2226, "2025 2nd Special Session", 2025, 1, "3HeWdk06sDfZCwL7Qycwse"),
    (2216, "2025 1st Special Session", 2025, 1, "4N4MdXODMmquUMCvk0xLg5"),
    (2169, "2025 Regular Session", 2025, 0, "3QLar05NbXxzYFzVdX1d79"),
    (2122, "2024 Regular Session", 2024, 0, "5VhQI5SgwnJHa5nTOdZi2z"),
    (2012, "2023 Regular Session", 2023, 0, "683C1lWKH97m9j6nFmqARn"),
    (1995, "2022 1st Extraordinary Session", 2022, 1, "3qNVXJ9nenKKbB48N5KIud"),
    (1957, "2022 Regular Session", 2022, 0, "3m33qn56NacVQnih16r9w1"),
    (1842, "2021 1st Extraordinary Session", 2021, 1, "2HIenVmmfNcQ2Uljtfmwyj"),
    (1790, "2021 Regular Session", 2021, 0, "6pO8ZWkmwunzlxWhFOMYib"),
    (1774, "2020 2nd Extraordinary Session", 2020, 1, "57sFQPj76GjtFzRQvbReha"),
    (1758, "2020 1st Extraordinary Session", 2020, 1, "4DvUz06dRoaGwqpbNWqAyD"),
    (1720, "2020 Regular Session", 2020, 0, "7KeYgVYi3VVFMSKaJKtwMb"),
    (1711, "2019 1st Extraordinary Session", 2019, 1, "5wykiCqEQxKorRQbcb9R64"),
    (1640, "2019 Regular Session", 2019, 0, "5jppcYkwbriYnSmW5pu45t"),
    (1608, "2018 2nd Extraordinary Session", 2018, 1, "3SJuUNuYUr4mXKcr6FUjIh"),
    (1579, "2018 1st Extraordinary Session", 2018, 1, "3BCezW2YX0h9qxlbsJ8CuO"),
    (1530, "2018 Regular Session", 2018, 0, "3IlBmmXiaoNalqF4JlqE3e"),
    (1465, "2017 2nd Extraordinary Session", 2017, 1, "5HzG4JrCVECyHtEEohiPSj"),
    (1460, "2017 1st Extraordinary Session", 2017, 1, "580nM6la85f8PKs9KH4xLY"),
    (1416, "2017 Regular Session", 2017, 0, "33io218hhUjeiDxX1ADPQA"),
    (1189, "2016 Regular Session", 2016, 0, "6TnffGj09Z4jdBt9ZZjTj9"),
    (1137, "2015 Regular Session", 2015, 0, "1g9NOF3nZkmzmhTXeMMi60"),
    (1070, "2014 Regular Session", 2014, 0, "4vfjc85tK4AX99JwuDYju"),
    (1094, "2013 1st Extraordinary Session", 2013, 1, "4ea2j4fons3T5HsvrhGVNT"),
    (1009, "2013 Regular Session", 2013, 0, "2G3sRvxBt8querPBlK8hO4"),
    (499,  "2012 Regular Session", 2012, 0, "1yIstbenWqzcBhFWkQgpzG"),
    (498,  "2011 1st Extraordinary Session", 2011, 1, "5VGJ3pfvdUqgmJ2tQyUxUY"),
    (97,   "2011 Regular Session", 2011, 0, "54QuuDvAObTCNGLAIQDJc3"),
    (497,  "2010 1st Extraordinary Session", 2010, 1, "2zGrvajrxsNVx2BXMF9TD7"),
    (71,   "2010 Regular Session", 2010, 0, "1kuYlbIxIg7Zw50hFz0bgs"),
]

CURRENT_SESSION_ID = 2239

STATUS_MAP = {
    1: "Introduced", 2: "Engrossed", 3: "Enrolled",
    4: "Passed", 5: "Vetoed", 6: "Failed"
}

def legiscan_call(op, **params):
    response = requests.get(BASE_URL, params={
        "key": API_KEY, "op": op, **params
    }, timeout=60)
    data = response.json()
    if data.get("status") == "ERROR":
        raise Exception(f"LegiScan error on {op}: {data.get('alert')}")
    return data

def get_current_people_ids():
    print("Fetching current session legislators for 'currently serving' flag...")
    data = legiscan_call("getSessionPeople", id=CURRENT_SESSION_ID)
    people = data.get("sessionpeople", {}).get("people", [])
    ids = {p["people_id"] for p in people}
    print(f"Found {len(ids)} currently serving legislators.")
    return ids

def get_progress(conn, session_id):
    c = conn.cursor()
    c.execute("SELECT * FROM fetch_progress WHERE session_id = ?", (session_id,))
    row = c.fetchone()
    return dict(row) if row else None

def mark_session_complete(conn, session_id):
    c = conn.cursor()
    c.execute("""
        INSERT OR REPLACE INTO fetch_progress (session_id, completed)
        VALUES (?, 1)
    """, (session_id,))
    conn.commit()

def download_zip(session_id, session_name, access_key):
    print(f"  Downloading dataset for {session_name}...")
    data = legiscan_call("getDataset", id=session_id, access_key=access_key)
    zip_b64 = data.get("dataset", {}).get("zip", "")
    if not zip_b64:
        print(f"  No zip data returned.")
        return None
    zip_bytes = base64.b64decode(zip_b64)
    return io.BytesIO(zip_bytes)

def store_session_data(conn, session_id, session_name, year, special,
                       zip_buffer, current_people_ids):
    c = conn.cursor()

    c.execute("""
        INSERT OR REPLACE INTO sessions (session_id, session_name, year, special)
        VALUES (?, ?, ?, ?)
    """, (session_id, session_name, year, special))

    legislators_seen = {}

    with zipfile.ZipFile(zip_buffer, "r") as zf:
        all_files = zf.namelist()

        bill_files  = [f for f in all_files if "/bill/" in f   and f.endswith(".json")]
        vote_files  = [f for f in all_files if "/vote/" in f   and f.endswith(".json")]
        people_files= [f for f in all_files if "/people/" in f and f.endswith(".json")]

        print(f"  Found {len(bill_files)} bills, {len(vote_files)} votes, {len(people_files)} people files.")

        # ---- PEOPLE ----
        # Parse people files first so we have rich legislator data
        for fname in people_files:
            try:
                with zf.open(fname) as f:
                    payload = json.load(f)
                person = payload.get("person", payload)
                pid = person.get("people_id")
                if pid:
                    legislators_seen[pid] = {
                        "people_id": pid,
                        "name": person.get("name", ""),
                        "party": person.get("party", ""),
                        "role": person.get("role", ""),
                        "district": str(person.get("district", "")),
                        "chamber": "House" if person.get("role") == "Rep" else "Senate" if person.get("role") == "Sen" else "",
                    }
            except Exception as e:
                continue

        # ---- BILLS ----
        for fname in bill_files:
            try:
                with zf.open(fname) as f:
                    payload = json.load(f)
                bill = payload.get("bill", payload)
                bill_id = bill.get("bill_id")
                if not bill_id:
                    continue

                status_text = STATUS_MAP.get(bill.get("status", 1), "Introduced")
                body_id = bill.get("body_id", 0)
                chamber = "House" if body_id == 1 else "Senate" if body_id == 2 else ""

                c.execute("""
                    INSERT OR REPLACE INTO bills
                        (bill_id, session_id, session_name, bill_number, title, status, chamber, url)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    bill_id, session_id, session_name,
                    bill.get("bill_number", ""),
                    bill.get("title", ""),
                    status_text, chamber,
                    bill.get("state_link", "")
                ))

                # Sponsors
                for sponsor in bill.get("sponsors", []):
                    pid = sponsor.get("people_id", 0)
                    stype = "Primary" if sponsor.get("sponsor_type_id") == 1 else "Co-Sponsor"
                    c.execute("""
                        INSERT OR IGNORE INTO sponsors
                            (bill_id, session_id, name, sponsor_type, people_id)
                        VALUES (?, ?, ?, ?, ?)
                    """, (bill_id, session_id, sponsor.get("name", ""), stype, pid))
                    if pid and pid not in legislators_seen:
                        legislators_seen[pid] = {
                            "people_id": pid,
                            "name": sponsor.get("name", ""),
                            "party": sponsor.get("party", ""),
                            "role": sponsor.get("role", ""),
                            "district": str(sponsor.get("district", "")),
                            "chamber": "House" if sponsor.get("role") == "Rep" else "Senate",
                        }

                # Committee
                committee_raw = bill.get("committee", {})
                if isinstance(committee_raw, list):
                    committee = committee_raw[0] if committee_raw else {}
                else:
                    committee = committee_raw or {}
                committee_name = committee.get("name", "").strip()
                if committee_name:
                    c.execute("""
                        INSERT OR IGNORE INTO committees
                            (bill_id, session_id, committee_name, chamber)
                        VALUES (?, ?, ?, ?)
                    """, (bill_id, session_id, committee_name, chamber))

                # Similar bills
                for sb in bill.get("sasts", []):
                    # sasts = "same as/similar to" — LegiScan's cross-session bill links
                    c.execute("""
                        INSERT OR IGNORE INTO similar_bills
                            (bill_id, similar_bill_id, similar_number, similar_title, similar_session)
                        VALUES (?, ?, ?, ?, ?)
                    """, (
                        bill_id,
                        sb.get("sast_bill_id", 0),
                        sb.get("sast_bill_number", ""),
                        sb.get("title", ""),
                        sb.get("sast_type", "")
                    ))

            except Exception as e:
                print(f"  Warning: could not parse bill {fname}: {e}")
                continue

        # ---- VOTES ----
        # Each vote file contains one roll call with all member votes inside it
        for fname in vote_files:
            try:
                with zf.open(fname) as f:
                    payload = json.load(f)
                rc = payload.get("roll_call", payload)
                roll_call_id = rc.get("roll_call_id")
                bill_id = rc.get("bill_id")
                if not roll_call_id or not bill_id:
                    continue

                rc_chamber_id = rc.get("chamber_id", 0)
                rc_chamber = "House" if rc_chamber_id == 1 else "Senate" if rc_chamber_id == 2 else rc.get("chamber", "")

                c.execute("""
                    INSERT OR REPLACE INTO votes
                        (roll_call_id, bill_id, session_id, date, description,
                         yea, nay, nv, passed, chamber)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    roll_call_id, bill_id, session_id,
                    rc.get("date", ""),
                    rc.get("desc", ""),
                    rc.get("yea", 0),
                    rc.get("nay", 0),
                    rc.get("nv", 0) + rc.get("absent", 0),
                    rc.get("passed", 0),
                    rc_chamber
                ))

                # Individual member votes are inside the roll call file
                for mv in rc.get("votes", []):
                    pid = mv.get("people_id")
                    c.execute("""
                        INSERT OR IGNORE INTO member_votes
                            (roll_call_id, people_id, session_id, vote_text)
                        VALUES (?, ?, ?, ?)
                    """, (
                        roll_call_id, pid, session_id,
                        mv.get("vote_text", "")
                    ))
                    # Collect legislator info from vote records
                    if pid and pid not in legislators_seen:
                        legislators_seen[pid] = {
                            "people_id": pid,
                            "name": mv.get("name", ""),
                            "party": mv.get("party", ""),
                            "role": "",
                            "district": "",
                            "chamber": "",
                        }

            except Exception as e:
                print(f"  Warning: could not parse vote file {fname}: {e}")
                continue

    # ---- LEGISLATORS ----
    for pid, person in legislators_seen.items():
        currently_serving = 1 if pid in current_people_ids else 0
        c.execute("""
            INSERT OR REPLACE INTO legislators
                (people_id, session_id, name, party, role, district, chamber, currently_serving)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            pid, session_id,
            person.get("name", ""),
            person.get("party", ""),
            person.get("role", ""),
            person.get("district", ""),
            person.get("chamber", ""),
            currently_serving
        ))

    conn.commit()
    print(f"  Stored {len(bill_files)} bills, {len(vote_files)} roll calls, {len(legislators_seen)} legislators.")

def run_historical_backfill():
    print("=== Missouri Vote Tracker: Historical Backfill via Datasets ===")
    print(f"Downloading {len(ALL_SESSIONS)} session datasets.")
    print("Safe to stop and restart — completed sessions are skipped automatically.")
    print("=" * 60)

    setup_history_database()
    conn = get_history_connection()
    current_people_ids = get_current_people_ids()

    for session_id, session_name, year, special, access_key in ALL_SESSIONS:
        print(f"\n--- {session_name} ---")
        progress = get_progress(conn, session_id)
        if progress and progress["completed"] == 1:
            print(f"  Already complete, skipping.")
            continue

        try:
            zip_buffer = download_zip(session_id, session_name, access_key)
            if not zip_buffer:
                mark_session_complete(conn, session_id)
                continue

            store_session_data(
                conn, session_id, session_name, year, special,
                zip_buffer, current_people_ids
            )
            mark_session_complete(conn, session_id)
            time.sleep(2)

        except Exception as e:
            print(f"  ERROR on {session_name}: {e}")
            continue

    conn.close()
    print("\n=== Historical backfill complete! ===")

if __name__ == "__main__":
    run_historical_backfill()