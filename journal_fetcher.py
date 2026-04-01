"""
journal_fetcher.py

Scrapes Missouri House journals (PDFs) directly from house.mo.gov as a
parallel data source alongside LegiScan. Extracts:
  - Roll call votes (bill passage votes with member-level AYES/NOES/PRESENT/ABSENT)
  - Bill second readings (bill number, title, sponsor)
  - Committee reports (committee, bill, recommendation, vote)

Run standalone:
    python journal_fetcher.py

Or import and call run_journal_fetch() from another script.

Data is merged into the existing SQLite database (mo_votes.db) via database.py.
"""

import re
import time
import requests
import pdfplumber
import io
from datetime import datetime, timezone
from database import get_connection, setup_database

# ── constants ────────────────────────────────────────────────────────────────

SESSION_CODE = "261"          # 2026 session — update each new session
BASE_PDF_URL = (
    f"https://documents.house.mo.gov/billtracking/"
    f"bills{SESSION_CODE}/jrnpdf/jrn{{:03d}}.pdf"
)
META_KEY_LAST_JOURNAL = "journal_last_fetched_journal_num"
META_KEY_UPDATED      = "journal_last_updated"

# ── database helpers ──────────────────────────────────────────────────────────

def ensure_journal_tables(conn):
    cursor = conn.cursor()

    # Track which journal PDFs we've already processed
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal_meta (
            journal_num  INTEGER PRIMARY KEY,
            journal_date TEXT,
            fetched_at   TEXT
        )
    """)

    # Roll call votes parsed from journals
    # journal_roll_call_id is our synthetic key: "J{journal_num}_{sequence}"
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal_votes (
            journal_roll_call_id TEXT PRIMARY KEY,
            journal_num          INTEGER,
            journal_date         TEXT,
            bill_number          TEXT,
            description          TEXT,
            yea                  INTEGER,
            nay                  INTEGER,
            present              INTEGER,
            absent               INTEGER,
            passed               INTEGER
        )
    """)

    # Member-level votes from journals
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal_member_votes (
            journal_roll_call_id TEXT,
            member_name          TEXT,
            vote_text            TEXT,
            PRIMARY KEY (journal_roll_call_id, member_name)
        )
    """)

    # Bill second readings (introduced bills with sponsor)
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal_bills (
            bill_number  TEXT PRIMARY KEY,
            title        TEXT,
            sponsor      TEXT,
            first_seen   TEXT
        )
    """)

    # Committee reports
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS journal_committee_reports (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            journal_num     INTEGER,
            journal_date    TEXT,
            committee_name  TEXT,
            bill_number     TEXT,
            recommendation  TEXT,
            ayes            INTEGER,
            noes            INTEGER
        )
    """)

    conn.commit()


def get_last_fetched_journal(conn):
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)
    """)
    cursor.execute("SELECT value FROM meta WHERE key = ?", (META_KEY_LAST_JOURNAL,))
    row = cursor.fetchone()
    return int(row[0]) if row else 0


def set_last_fetched_journal(conn, num):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)
    """, (META_KEY_LAST_JOURNAL, str(num)))
    cursor.execute("""
        INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)
    """, (META_KEY_UPDATED, datetime.now(timezone.utc).isoformat()))
    conn.commit()


def journal_already_fetched(conn, journal_num):
    cursor = conn.cursor()
    cursor.execute(
        "SELECT 1 FROM journal_meta WHERE journal_num = ?", (journal_num,)
    )
    return cursor.fetchone() is not None


# ── PDF fetching ──────────────────────────────────────────────────────────────

def fetch_pdf_text(journal_num):
    """Download journal PDF and extract full text. Returns None if not found."""
    url = BASE_PDF_URL.format(journal_num)
    try:
        resp = requests.get(url, timeout=30)
        if resp.status_code == 404:
            return None, url
        resp.raise_for_status()
        pdf_bytes = io.BytesIO(resp.content)
        pages = []
        with pdfplumber.open(pdf_bytes) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    pages.append(text)
        return "\n".join(pages), url
    except requests.HTTPError:
        return None, url
    except Exception as e:
        print(f"  Error fetching journal {journal_num}: {e}")
        return None, url


# ── parsing ───────────────────────────────────────────────────────────────────

def parse_journal_date(text):
    """Extract the session date from journal header, e.g. 'MONDAY, MARCH 2, 2026'."""
    match = re.search(
        r"(?:MONDAY|TUESDAY|WEDNESDAY|THURSDAY|FRIDAY|SATURDAY|SUNDAY),\s+"
        r"([A-Z]+ \d{1,2}, \d{4})",
        text
    )
    if match:
        try:
            return datetime.strptime(match.group(1), "%B %d, %Y").strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def parse_name_block(text):
    """
    Parse a whitespace-separated block of representative last names.
    Names may include a district number suffix like 'Jones 12' or 'Smith 68'.
    Returns a list of name strings.
    """
    # Split on 2+ spaces or newlines to separate name columns
    tokens = re.split(r"  +|\n", text.strip())
    names = []
    i = 0
    while i < len(tokens):
        token = tokens[i].strip()
        if not token:
            i += 1
            continue
        # Check if next token is a short number (district suffix)
        if i + 1 < len(tokens):
            nxt = tokens[i + 1].strip()
            if re.match(r"^\d{1,3}$", nxt):
                names.append(f"{token} {nxt}")
                i += 2
                continue
        names.append(token)
        i += 1
    return [n for n in names if n and not re.match(r"^\d+$", n)]


def parse_roll_calls(text, journal_num, journal_date):
    """
    Find all roll call votes in the journal text.
    Looks for patterns like:
      HB 1234, relating to X, was taken up ...
      passed by the following vote:
      AYES: 148
      Allen Amato ...
      NOES: 000
      ...
    Returns list of dicts.
    """
    roll_calls = []

    # Split text into sections around "following vote:"
    sections = re.split(r"(?=by the following vote:)", text)

    for i, section in enumerate(sections[1:], start=1):
        # Look backwards in previous section for bill reference and description
        prev = sections[i - 1]

        # Extract bill number and action from context
        bill_match = re.search(
            r"(H[BCR]{1,2}s?\s[\d,&\s]+(?:&\s*\d+)?)\s*,\s*relating to ([^\n,]+)",
            prev[-500:]
        )
        bill_number = bill_match.group(1).strip() if bill_match else "Unknown"
        description = bill_match.group(2).strip() if bill_match else ""

        # Determine if passed or failed
        passed_match = re.search(r"(passed|failed|adopted|rejected)", prev[-200:], re.I)
        passed = 1 if passed_match and passed_match.group(1).lower() in ("passed", "adopted") else 0
        # Also check section itself
        if re.search(r"declared the bill passed", section[:200], re.I):
            passed = 1

        # Extract vote counts and name blocks
        ayes_match    = re.search(r"AYES:\s*(\d+)", section)
        noes_match    = re.search(r"NOES:\s*(\d+)", section)
        present_match = re.search(r"PRESENT:\s*(\d+)", section)
        absent_match  = re.search(r"ABSENT WITH LEAVE:\s*(\d+)", section)

        if not ayes_match:
            continue

        yea     = int(ayes_match.group(1))
        nay     = int(noes_match.group(1))     if noes_match    else 0
        present = int(present_match.group(1))  if present_match else 0
        absent  = int(absent_match.group(1))   if absent_match  else 0

        # Extract member names per category
        def extract_names(label, text):
            pattern = rf"{label}:\s*\d+\s*(.*?)(?=\n(?:AYES|NOES|PRESENT|ABSENT WITH LEAVE|VACANCIES|Mr\. Speaker|[A-Z]{{4}})|$)"
            m = re.search(pattern, text, re.S)
            if not m:
                return []
            return parse_name_block(m.group(1))

        ayes_names    = extract_names("AYES", section)
        noes_names    = extract_names("NOES", section)
        present_names = extract_names("PRESENT", section)
        absent_names  = extract_names("ABSENT WITH LEAVE", section)

        seq_id = f"J{journal_num}_{i}"
        roll_calls.append({
            "journal_roll_call_id": seq_id,
            "journal_num":  journal_num,
            "journal_date": journal_date,
            "bill_number":  bill_number,
            "description":  description,
            "yea":          yea,
            "nay":          nay,
            "present":      present,
            "absent":       absent,
            "passed":       passed,
            "members": (
                [(n, "Yea")    for n in ayes_names] +
                [(n, "Nay")    for n in noes_names] +
                [(n, "NV")     for n in present_names] +
                [(n, "Absent") for n in absent_names]
            )
        })

    return roll_calls


def parse_second_readings(text, journal_date):
    """
    Parse SECOND READING OF HOUSE BILLS sections.
    Format: HB 1234, introduced by Representative Smith, relating to X.
    Also handles: HB 1234, relating to X. (no explicit sponsor listed inline —
    sponsor extracted from 'Sponsor: Name' pattern on Session page style lines)
    """
    bills = []

    # Pattern: HB/HJR/HCR NNNN, introduced by Representative Surname, relating to Title
    pattern = re.compile(
        r"(H[BCR]{1,2}s?\s[\d,&\s]+(?:&\s*\d+)?),\s*"
        r"(?:introduced by Representative ([A-Za-z\s\(\)]+?),\s*)?"
        r"relating to ([^\n.]+)",
        re.I
    )
    for m in pattern.finditer(text):
        bill_number = m.group(1).strip()
        sponsor     = m.group(2).strip() if m.group(2) else ""
        title       = m.group(3).strip()
        bills.append({
            "bill_number": bill_number,
            "title":       title,
            "sponsor":     sponsor,
            "first_seen":  journal_date,
        })

    return bills


def parse_committee_reports(text, journal_num, journal_date):
    """
    Parse committee report blocks like:
      Committee on Fiscal Review, Chairman Murphy reporting:
      Mr. Speaker: Your Committee on Fiscal Review, to which was referred HCS HB 1948,
      begs leave to report it has examined the same and recommends that it Do Pass by the following vote:
      Ayes (7): Casteel, Fogle, ...
      Noes (0)
    """
    reports = []

    # Find committee name
    committee_header = re.compile(
        r"Committee on ([^\n,]+),\s*Chairman [^\n]+ reporting:"
    )

    # Find each report block
    report_block = re.compile(
        r"referred\s+(H[^\s,]+(?:\s+H[^\s,]+)*),\s*"
        r"begs leave[^\n]*recommends[^\n]*\n"
        r"Ayes\s*\((\d+)\)[^\n]*\n"
        r"Noes\s*\((\d+)\)",
        re.S
    )

    # Get active committee name at each point
    last_committee = "Unknown"
    for segment in re.split(r"\n(?=Committee on )", text):
        ch = committee_header.search(segment)
        if ch:
            last_committee = ch.group(1).strip()

        for m in report_block.finditer(segment):
            bill   = m.group(1).strip()
            ayes   = int(m.group(2))
            noes   = int(m.group(3))
            rec_match = re.search(r"recommends that it (Do Pass|Do Not Pass|be placed)", m.group(0), re.I)
            recommendation = rec_match.group(1) if rec_match else "Do Pass"
            reports.append({
                "journal_num":    journal_num,
                "journal_date":   journal_date,
                "committee_name": last_committee,
                "bill_number":    bill,
                "recommendation": recommendation,
                "ayes":           ayes,
                "noes":           noes,
            })

    return reports


# ── database writes ───────────────────────────────────────────────────────────

def store_roll_calls(conn, roll_calls):
    cursor = conn.cursor()
    for rc in roll_calls:
        cursor.execute("""
            INSERT OR IGNORE INTO journal_votes
                (journal_roll_call_id, journal_num, journal_date, bill_number,
                 description, yea, nay, present, absent, passed)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            rc["journal_roll_call_id"], rc["journal_num"], rc["journal_date"],
            rc["bill_number"], rc["description"],
            rc["yea"], rc["nay"], rc["present"], rc["absent"], rc["passed"]
        ))
        for name, vote_text in rc["members"]:
            cursor.execute("""
                INSERT OR IGNORE INTO journal_member_votes
                    (journal_roll_call_id, member_name, vote_text)
                VALUES (?, ?, ?)
            """, (rc["journal_roll_call_id"], name, vote_text))
    conn.commit()


def store_bills(conn, bills):
    cursor = conn.cursor()
    for b in bills:
        cursor.execute("""
            INSERT OR IGNORE INTO journal_bills
                (bill_number, title, sponsor, first_seen)
            VALUES (?, ?, ?, ?)
        """, (b["bill_number"], b["title"], b["sponsor"], b["first_seen"]))
    conn.commit()


def store_committee_reports(conn, reports):
    cursor = conn.cursor()
    for r in reports:
        cursor.execute("""
            INSERT INTO journal_committee_reports
                (journal_num, journal_date, committee_name, bill_number,
                 recommendation, ayes, noes)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            r["journal_num"], r["journal_date"], r["committee_name"],
            r["bill_number"], r["recommendation"], r["ayes"], r["noes"]
        ))
    conn.commit()


def mark_journal_fetched(conn, journal_num, journal_date):
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO journal_meta (journal_num, journal_date, fetched_at)
        VALUES (?, ?, ?)
    """, (journal_num, journal_date, datetime.now(timezone.utc).isoformat()))
    conn.commit()


# ── main loop ─────────────────────────────────────────────────────────────────

def run_journal_fetch(start_from=1, max_consecutive_misses=3):
    """
    Fetch and parse all new House journal PDFs.
    Stops after `max_consecutive_misses` consecutive missing journal numbers
    (indicating we've reached the end of published journals).
    """
    print("=== Missouri Journal Fetcher: Starting ===")
    setup_database()
    conn = get_connection()
    ensure_journal_tables(conn)

    last_fetched = get_last_fetched_journal(conn)
    start = max(start_from, last_fetched + 1)
    print(f"Starting from journal #{start} (last fetched: #{last_fetched})")

    consecutive_misses = 0
    journal_num = start
    total_votes = 0
    total_bills = 0
    total_reports = 0

    while consecutive_misses < max_consecutive_misses:
        if journal_already_fetched(conn, journal_num):
            print(f"  Journal #{journal_num:03d}: already in DB, skipping")
            journal_num += 1
            consecutive_misses = 0
            continue

        print(f"  Journal #{journal_num:03d}: fetching PDF...", end=" ", flush=True)
        text, url = fetch_pdf_text(journal_num)

        if text is None:
            print("not found")
            consecutive_misses += 1
            journal_num += 1
            continue

        consecutive_misses = 0
        journal_date = parse_journal_date(text) or "Unknown"
        print(f"ok ({journal_date})")

        roll_calls = parse_roll_calls(text, journal_num, journal_date)
        bills      = parse_second_readings(text, journal_date)
        reports    = parse_committee_reports(text, journal_num, journal_date)

        store_roll_calls(conn, roll_calls)
        store_bills(conn, bills)
        store_committee_reports(conn, reports)
        mark_journal_fetched(conn, journal_num, journal_date)
        set_last_fetched_journal(conn, journal_num)

        print(f"    → {len(roll_calls)} roll calls, {len(bills)} bills, {len(reports)} committee reports")
        total_votes   += len(roll_calls)
        total_bills   += len(bills)
        total_reports += len(reports)

        journal_num += 1
        time.sleep(0.5)   # be polite to the server

    conn.close()
    print(f"\n=== Journal fetch complete ===")
    print(f"  Roll calls parsed : {total_votes}")
    print(f"  Bills parsed      : {total_bills}")
    print(f"  Committee reports : {total_reports}")


if __name__ == "__main__":
    run_journal_fetch()