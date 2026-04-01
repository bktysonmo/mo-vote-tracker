import os
import re
import time
import sqlite3
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv

load_dotenv()

CURRENT_YEAR = 2026
CURRENT_ASSEMBLY = 261

HOUSE_PDF_VERSIONS = ["T", "C", "S", "I"]

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

SIMILAR_PHRASES = [
    "is similar to",
    "is identical to",
    "is substantially similar to",
    "contains provisions similar to",
    "to provisions in",
    "similar to provisions in",
]

BILL_REF_PATTERN = re.compile(
    r'((?:HCS|SCS|SS|CCS|HB|SB|HJR|SJR|SCR|HCR)(?:/(?:HCS|SCS|SS|CCS|HB|SB|HJR|SJR))*'
    r'\s*\d+)\s*\((\d{4})\)',
    re.IGNORECASE
)

def get_db():
    conn = sqlite3.connect("mo_votes.db")
    conn.row_factory = sqlite3.Row
    return conn

def setup_similar_bills_table():
    conn = get_db()
    c = conn.cursor()
    c.execute("""
        CREATE TABLE IF NOT EXISTS similar_bills (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            bill_id         INTEGER,
            bill_number     TEXT,
            similar_number  TEXT,
            similar_year    TEXT,
            relationship    TEXT,
            source_version  TEXT,
            UNIQUE(bill_id, similar_number, similar_year)
        )
    """)
    c.execute("""
        CREATE TABLE IF NOT EXISTS bill_summaries (
            bill_id         INTEGER PRIMARY KEY,
            bill_number     TEXT,
            summary_text    TEXT,
            source_version  TEXT,
            fetched_at      TEXT
        )
    """)
    conn.commit()
    conn.close()
    print("Similar bills and summaries tables ready.")

def extract_similar_bills(text, bill_number):
    results = []
    sentences = re.split(r'(?<=[.!?])\s+', text)
    for sentence in sentences:
        sentence_lower = sentence.lower()
        relationship = None
        for phrase in SIMILAR_PHRASES:
            if phrase in sentence_lower:
                relationship = phrase
                break
        if not relationship:
            continue
        for match in BILL_REF_PATTERN.finditer(sentence):
            ref_bill = match.group(1).strip()
            ref_year = match.group(2)
            if ref_bill.replace(" ", "").upper() == bill_number.replace(" ", "").upper():
                continue
            results.append({
                "similar_number": ref_bill,
                "similar_year": ref_year,
                "relationship": relationship
            })
    return results

CHAPTER_PATTERN = re.compile(
    r'(?:'
    r'chapters?\s+(\d{2,3})(?:\s*,\s*(\d{2,3}))*(?:\s+and\s+(\d{2,3}))?'  # "chapters 208 and 209" or "chapters 208, 209"
    r'|section\s+(\d{2,3})\.\d+'
    r'|\b(\d{2,3})(?:\.\d+)+\s*,?\s*RSMo'
    r')',
    re.IGNORECASE
)

def extract_chapters(text):
    chapters = set()
    # Handle "chapters X and Y" separately since it needs findall on the number list
    for m in re.finditer(r'chapters?\s+([\d,\s]+(?:and\s+\d+)?)', text, re.IGNORECASE):
        for ch in re.findall(r'\d{2,3}', m.group(1)):
            chapters.add(ch.lstrip('0') or '0')
    # Handle section X.Y and X.Y RSMo
    for m in re.finditer(r'section\s+(\d{2,3})\.\d+|\b(\d{2,3})(?:\.\d+)+\s*,?\s*RSMo', text, re.IGNORECASE):
        ch = m.group(1) or m.group(2)
        if ch:
            chapters.add(ch.lstrip('0') or '0')
    return sorted(chapters)

def fetch_house_summary(bill_number, assembly=CURRENT_ASSEMBLY):
    try:
        import pdfplumber
    except ImportError:
        print("  pdfplumber not installed. Run: pip3 install pdfplumber")
        return None, None

    bill_clean = bill_number.replace(" ", "")
    for version in HOUSE_PDF_VERSIONS:
        url = f"https://documents.house.mo.gov/billtracking/bills{assembly}/sumpdf/{bill_clean}{version}.pdf"
        try:
            r = requests.get(url, timeout=10, headers=HEADERS)
            if r.status_code == 200 and len(r.content) > 500:
                import io
                with pdfplumber.open(io.BytesIO(r.content)) as pdf:
                    text = "\n".join(page.extract_text() or "" for page in pdf.pages)
                if text.strip():
                    return text, version
        except Exception:
            continue
    return None, None

def build_senate_bill_map(year=CURRENT_YEAR):
    print(f"  Building Senate bill ID map for {year}...")
    url = f"https://www.senate.mo.gov/BillTracking/Bills/BillList?year={year}&session=R"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        bill_map = {}

        # Match the exact pattern we see in the HTML:
        # href="...BillInformation?year=2026&amp;billid=416" ...>SB 834</a>
        pattern = re.compile(
            r'BillInformation\?year=\d+&amp;billid=(\d+)[^>]*>\s*(S[BJ][R]?\s*\d+)\s*</a>',
            re.IGNORECASE
        )
        for match in pattern.finditer(r.text):
            bill_id = match.group(1)
            bill_num = match.group(2).strip()
            bill_map[bill_num] = bill_id

        print(f"  Found {len(bill_map)} Senate bills in map.")
        if bill_map:
            print(f"  Sample: {list(bill_map.items())[:3]}")
        return bill_map
    except Exception as e:
        print(f"  Error building Senate bill map: {e}")
        return {}

def fetch_senate_summary(bill_number, bill_map):
    bill_id = bill_map.get(bill_number)
    if not bill_id:
        normalized = re.sub(r'([A-Z]+)(\d+)', r'\1 \2', bill_number.replace(" ", ""))
        bill_id = bill_map.get(normalized)
    if not bill_id:
        return None, None

    url = f"https://www.senate.mo.gov/BillTracking/Bills/BillInformation?year={CURRENT_YEAR}&billid={bill_id}"
    try:
        r = requests.get(url, timeout=10, headers=HEADERS)
        if r.status_code != 200:
            return None, None
        soup = BeautifulSoup(r.text, "html.parser")
        full_text = soup.get_text(separator=" ", strip=True)
        summary_match = re.search(
            r'CURRENT BILL SUMMARY\s*(.*?)(?:OLIVIA|PREPARED BY|\u2715|$)',
            full_text, re.DOTALL | re.IGNORECASE
        )
        if summary_match:
            return summary_match.group(1).strip(), "web"
        return full_text, "web"
    except Exception as e:
        return None, None

def run_summary_fetch():
    print("=== Missouri Vote Tracker: Summary & Similar Bills Fetcher ===")

    try:
        import pdfplumber
    except ImportError:
        print("\nERROR: pdfplumber not installed.")
        print("Run: pip3 install pdfplumber beautifulsoup4")
        return

    setup_similar_bills_table()
    conn = get_db()
    c = conn.cursor()

    c.execute("SELECT bill_id, bill_number, chamber FROM bills ORDER BY chamber, bill_number")
    all_bills = c.fetchall()
    print(f"Found {len(all_bills)} bills to process.")

    senate_map = build_senate_bill_map()

    house_count = 0
    senate_count = 0
    similar_count = 0

    for i, bill in enumerate(all_bills):
        bill_id = bill["bill_id"]
        bill_number = bill["bill_number"]
        chamber = bill["chamber"]

        c.execute("SELECT 1 FROM bill_summaries WHERE bill_id = ?", (bill_id,))
        if c.fetchone():
            continue

        summary_text = None
        source_version = None

        if chamber == "House" or bill_number.upper().startswith("HB") or bill_number.upper().startswith("HJR"):
            summary_text, source_version = fetch_house_summary(bill_number)
            if summary_text:
                house_count += 1
        elif chamber == "Senate" or bill_number.upper().startswith("SB") or bill_number.upper().startswith("SJR"):
            summary_text, source_version = fetch_senate_summary(bill_number, senate_map)
            if summary_text:
                senate_count += 1

        if summary_text:
            from datetime import datetime
            c.execute("""
                INSERT OR REPLACE INTO bill_summaries
                    (bill_id, bill_number, summary_text, source_version, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            """, (bill_id, bill_number, summary_text, source_version, datetime.now().isoformat()))

            similar_refs = extract_similar_bills(summary_text, bill_number)
            for ref in similar_refs:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO similar_bills
                            (bill_id, bill_number, similar_number, similar_year, relationship, source_version)
                        VALUES (?, ?, ?, ?, ?, ?)
                    """, (bill_id, bill_number, ref["similar_number"],
                          ref["similar_year"], ref["relationship"], source_version))
                    similar_count += 1
                except Exception:
                    pass

            c.execute("""
                CREATE TABLE IF NOT EXISTS bill_chapters (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    bill_id     INTEGER,
                    bill_number TEXT,
                    chapter     TEXT,
                    UNIQUE(bill_id, chapter)
                )
            """)
            
            chapter_list = extract_chapters(summary_text)
            for ch in chapter_list:
                try:
                    c.execute("""
                        INSERT OR IGNORE INTO bill_chapters
                            (bill_id, bill_number, chapter)
                        VALUES (?, ?, ?)
                """, (bill_id, bill_number, ch))
                except Exception:
                    pass

            conn.commit()

        time.sleep(0.3)

        if (i + 1) % 50 == 0:
            print(f"  Progress: {i+1}/{len(all_bills)} | House: {house_count} | Senate: {senate_count} | Similar refs: {similar_count}")

    conn.close()
    print(f"\n=== Complete ===")
    print(f"House summaries: {house_count}")
    print(f"Senate summaries: {senate_count}")
    print(f"Similar bill references: {similar_count}")

if __name__ == "__main__":
    run_summary_fetch()