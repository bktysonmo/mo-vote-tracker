import os
import re
import io
import time
import sqlite3
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from database import get_connection, setup_database

load_dotenv()

CURRENT_YEAR = 2026
CURRENT_ASSEMBLY = 261

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
}

def get_senate_bill_map():
    print("  Building Senate bill ID map...")
    url = f"https://www.senate.mo.gov/BillTracking/Bills/BillList?year={CURRENT_YEAR}&session=R"
    try:
        r = requests.get(url, timeout=15, headers=HEADERS)
        bill_map = {}
        pattern = re.compile(
            r'BillInformation\?year=\d+&amp;billid=(\d+)[^>]*>\s*(S[BJ][R]?\s*\d+)\s*</a>',
            re.IGNORECASE
        )
        for match in pattern.finditer(r.text):
            bill_map[match.group(2).strip()] = match.group(1)
        print(f"  Found {len(bill_map)} Senate bills.")
        return bill_map
    except Exception as e:
        print(f"  Error: {e}")
        return {}

def fetch_senate_amendments(bill_number, bill_id_str):
    url = f"https://www.senate.mo.gov/BillTracking/Bills/BillInformation?year={CURRENT_YEAR}&billid={bill_id_str}"
    try:
        r = requests.get(url, timeout=10, headers=HEADERS)
        if r.status_code != 200:
            return []
        soup = BeautifulSoup(r.text, "html.parser")

        amendments = []
        for link in soup.find_all("a", href=True):
            href = link["href"]
            if "handler=AmendmentPdf" not in href:
                continue

            amendment_id_match = re.search(r'amendmentId=(\d+)', href)
            if not amendment_id_match:
                continue

            amendment_id = amendment_id_match.group(1)
            pdf_url = f"https://www.senate.mo.gov/BillTracking/Bills/BillInformation?handler=AmendmentPdf&year={CURRENT_YEAR}&amendmentId={amendment_id}"

            link_text = link.get_text(separator="\n", strip=True)
            lines = [l.strip() for l in link_text.split("\n") if l.strip()]

            amendment_code = ""
            amendment_name = ""
            status = ""

            if lines:
                first = lines[0]
                if " - " in first:
                    parts = first.split(" - ", 1)
                    amendment_code = parts[0].strip()
                    amendment_name = parts[1].strip()
                else:
                    amendment_code = first

            full_text_lower = link_text.lower()
            if "adopted" in full_text_lower:
                status = "Adopted"
            elif "failed" in full_text_lower:
                status = "Failed"
            elif "withdrawn" in full_text_lower:
                status = "Withdrawn"
            else:
                status = "Pending"

            parent = link.parent
            if parent:
                parent_text = parent.get_text(separator=" ", strip=True)
                if "adopted, as amended" in parent_text.lower():
                    status = "Adopted as Amended"
                elif "adopted" in parent_text.lower():
                    status = "Adopted"
                elif "failed" in parent_text.lower():
                    status = "Failed"

            amendments.append({
                "amendment_code": amendment_code,
                "amendment_name": amendment_name,
                "status": status,
                "pdf_url": pdf_url,
                "sponsor": "",
                "floor_number": ""
            })

        return amendments
    except Exception as e:
        print(f"    Senate amendment fetch error for {bill_number}: {e}")
        return []

def fetch_house_amendments(bill_number):
    bill_clean = bill_number.replace(" ", "")
    url = f"https://www.house.mo.gov/amendments.aspx?bill={bill_clean}&year={CURRENT_YEAR}&code=R"
    try:
        r = requests.get(url, timeout=10, headers=HEADERS)
        if r.status_code != 200:
            return []

        soup = BeautifulSoup(r.text, "html.parser")
        amendments = []

        table = soup.find("table")
        if not table:
            return []

        rows = table.find_all("tr")
        for row in rows[1:]:
            cells = row.find_all("td")
            if len(cells) < 3:
                continue

            status = cells[0].get_text(strip=True)
            amendment_cell = cells[1]
            sponsor_cell = cells[2]
            floor_cell = cells[3] if len(cells) > 3 else None

            amendment_link = amendment_cell.find("a")
            if not amendment_link:
                continue

            amendment_code = amendment_link.get_text(strip=True)
            sponsor = sponsor_cell.get_text(strip=True)
            floor_number = floor_cell.get_text(strip=True) if floor_cell else ""

            pdf_url = f"https://documents.house.mo.gov/billtracking/bills{CURRENT_ASSEMBLY}/amendpdf/{amendment_code}.pdf"

            amendments.append({
                "amendment_code": amendment_code,
                "amendment_name": "",
                "status": status,
                "pdf_url": pdf_url,
                "sponsor": sponsor,
                "floor_number": floor_number
            })

        return amendments
    except Exception as e:
        print(f"    House amendment fetch error for {bill_number}: {e}")
        return []

def fetch_amendment_text(pdf_url):
    try:
        import pdfplumber
        r = requests.get(pdf_url, timeout=15, headers=HEADERS)
        if r.status_code != 200 or len(r.content) < 500:
            return ""
        with pdfplumber.open(io.BytesIO(r.content)) as pdf:
            text = "\n".join(page.extract_text() or "" for page in pdf.pages)
        return text.strip()
    except Exception:
        return ""

def already_fetched(conn, bill_number, amendment_code):
    cursor = conn.cursor()
    cursor.execute("""
        SELECT 1 FROM amendments
        WHERE bill_number = ? AND amendment_code = ? AND full_text != ''
    """, (bill_number, amendment_code))
    return cursor.fetchone() is not None

def store_amendment(conn, bill_id, bill_number, chamber, amendment):
    from datetime import datetime
    cursor = conn.cursor()
    cursor.execute("""
        INSERT OR REPLACE INTO amendments
            (bill_id, bill_number, chamber, amendment_code, amendment_name,
             sponsor, floor_number, status, full_text, pdf_url, fetched_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        bill_id,
        bill_number,
        chamber,
        amendment["amendment_code"],
        amendment["amendment_name"],
        amendment["sponsor"],
        amendment["floor_number"],
        amendment["status"],
        amendment.get("full_text", ""),
        amendment["pdf_url"],
        datetime.now().isoformat()
    ))
    conn.commit()

def run_amendment_fetch(senate_only=False):
    print("=== Missouri Vote Tracker: Amendment Fetcher ===")

    try:
        import pdfplumber
    except ImportError:
        print("ERROR: pdfplumber not installed. Run: pip3 install pdfplumber")
        return

    setup_database()
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT bill_id, bill_number, chamber
        FROM bills
        ORDER BY chamber, bill_number
    """)
    all_bills = cursor.fetchall()

    if senate_only:
        all_bills = [b for b in all_bills if b["chamber"] == "Senate"]
        print(f"Senate-only mode: {len(all_bills)} Senate bills.")
    else:
        print(f"Processing {len(all_bills)} bills for amendments.")

    senate_map = get_senate_bill_map()

    house_count = 0
    senate_count = 0
    amendment_count = 0
    pdf_count = 0

    for i, bill in enumerate(all_bills):
        bill_id = bill["bill_id"]
        bill_number = bill["bill_number"]
        chamber = bill["chamber"]

        amendments = []

        if chamber == "House" or bill_number.upper().startswith("HB") or bill_number.upper().startswith("HJR"):
            amendments = fetch_house_amendments(bill_number)
            if amendments:
                house_count += 1
        elif chamber == "Senate" or bill_number.upper().startswith("SB") or bill_number.upper().startswith("SJR"):
            # Normalize bill number: "SB885" -> "SB 885" to match senate map keys
            normalized = re.sub(r'^(S[BJR]+)(\d)', r'\1 \2', bill_number)
            bill_id_str = senate_map.get(normalized) or senate_map.get(bill_number)
            if bill_id_str:
                amendments = fetch_senate_amendments(bill_number, bill_id_str)
                if amendments:
                    senate_count += 1

        for amendment in amendments:
            amendment_count += 1
            if not already_fetched(conn, bill_number, amendment["amendment_code"]):
                text = fetch_amendment_text(amendment["pdf_url"])
                amendment["full_text"] = text
                if text:
                    pdf_count += 1
                time.sleep(0.3)
            else:
                amendment["full_text"] = ""

            store_amendment(conn, bill_id, bill_number, chamber, amendment)

        time.sleep(0.2)

        if (i + 1) % 100 == 0:
            print(f"  Progress: {i+1}/{len(all_bills)} | Amendments found: {amendment_count} | PDFs fetched: {pdf_count}")

    conn.close()
    print(f"\n=== Amendment fetch complete ===")
    print(f"House bills with amendments: {house_count}")
    print(f"Senate bills with amendments: {senate_count}")
    print(f"Total amendments stored: {amendment_count}")
    print(f"Amendment PDFs fetched: {pdf_count}")

if __name__ == "__main__":
    run_amendment_fetch(senate_only=True)