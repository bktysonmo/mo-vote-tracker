import streamlit as st
import pandas as pd
from database import get_connection
from pdf_reports import (
    generate_legislator_voting_record,
    generate_legislator_party_line_report,
    generate_bill_vote_report,
    generate_bill_party_line_report
)

st.set_page_config(
    page_title="Missouri Vote Tracker",
    page_icon="🏛️",
    layout="wide"
)

st.title("🏛️ Missouri Vote Tracker")
st.caption("Tracking votes in the Missouri House and Senate")

# ----------------------- DATABASE QUERIES -----------------------
def get_legislators():
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT people_id, name, party, chamber, district
        FROM legislators
        ORDER BY chamber, name
    """, conn)
    conn.close()
    return df

def get_voting_record(people_id):
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            b.bill_number,
            b.title,
            v.date,
            v.description,
            v.chamber,
            v.passed,
            mv.vote_text,
            b.url
        FROM member_votes mv
        JOIN votes v ON mv.roll_call_id = v.roll_call_id
        JOIN bills b ON v.bill_id = b.bill_id
        WHERE mv.people_id = ?
        ORDER BY v.date DESC
    """, conn, params=(people_id,))
    conn.close()
    return df

def get_vote_summary(people_id):
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT vote_text, COUNT(*) as count
        FROM member_votes
        WHERE people_id = ?
        GROUP BY vote_text
        ORDER BY count DESC
    """, conn, params=(people_id,))
    conn.close()
    return df

def get_bills_with_votes():
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT DISTINCT b.bill_id, b.bill_number, b.title, b.url
        FROM bills b
        JOIN votes v ON b.bill_id = v.bill_id
        ORDER BY b.bill_number
    """, conn)
    conn.close()
    return df

def get_bill_votes(bill_id):
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            v.roll_call_id,
            v.date,
            v.description,
            v.chamber,
            v.yea,
            v.nay,
            v.nv,
            v.passed
        FROM votes v
        WHERE v.bill_id = ?
        ORDER BY v.date DESC
    """, conn, params=(bill_id,))
    conn.close()
    return df

def get_roll_call_detail(roll_call_id):
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT
            l.name,
            l.party,
            l.chamber,
            l.district,
            mv.vote_text
        FROM member_votes mv
        JOIN legislators l ON mv.people_id = l.people_id
        WHERE mv.roll_call_id = ?
        ORDER BY l.party, l.name
    """, conn, params=(roll_call_id,))
    conn.close()
    return df

def calculate_party_line(roll_call_df):
    results = []
    parties = roll_call_df["party"].unique()
    for party in sorted(parties):
        if party == "":
            continue
        party_votes = roll_call_df[roll_call_df["party"] == party]
        total = len(party_votes)
        yea_count = len(party_votes[party_votes["vote_text"] == "Yea"])
        nay_count = len(party_votes[party_votes["vote_text"] == "Nay"])
        nv_count = len(party_votes[party_votes["vote_text"].isin(["NV", "Absent"])])
        if yea_count >= nay_count:
            party_line = "Yea"
            unity = round((yea_count / total) * 100, 1) if total > 0 else 0
        else:
            party_line = "Nay"
            unity = round((nay_count / total) * 100, 1) if total > 0 else 0
        broke_rank = party_votes[
            (party_votes["vote_text"].isin(["Yea", "Nay"])) &
            (party_votes["vote_text"] != party_line)
        ]["name"].tolist()
        results.append({
            "Party": party,
            "Total Members": total,
            "Yea": yea_count,
            "Nay": nay_count,
            "NV/Absent": nv_count,
            "Party Line": party_line,
            "Unity %": unity,
            "Broke Rank": ", ".join(broke_rank) if broke_rank else "None"
        })
    return pd.DataFrame(results)

def build_legislator_party_line_df(people_id, party, record_df):
    conn = get_connection()
    rows = []
    for _, vote_row in record_df.iterrows():
        rc = pd.read_sql_query("""
            SELECT mv.roll_call_id
            FROM member_votes mv
            JOIN votes v ON mv.roll_call_id = v.roll_call_id
            JOIN bills b ON v.bill_id = b.bill_id
            WHERE mv.people_id = ?
            AND b.bill_number = ?
            AND v.date = ?
            AND v.description = ?
            LIMIT 1
        """, conn, params=(
            people_id,
            vote_row["bill_number"],
            vote_row["date"],
            vote_row["description"]
        ))
        if rc.empty:
            continue
        roll_call_id = int(rc.iloc[0]["roll_call_id"])
        detail = get_roll_call_detail(roll_call_id)
        if detail.empty:
            continue
        party_votes = detail[detail["party"] == party]
        if party_votes.empty:
            continue
        yea_count = len(party_votes[party_votes["vote_text"] == "Yea"])
        nay_count = len(party_votes[party_votes["vote_text"] == "Nay"])
        total = len(party_votes)
        party_line = "Yea" if yea_count >= nay_count else "Nay"
        unity = round((max(yea_count, nay_count) / total) * 100, 1) if total > 0 else 0
        member_vote = vote_row["vote_text"]
        broke_rank = member_vote in ["Yea", "Nay"] and member_vote != party_line
        rows.append({
            "bill_number": vote_row["bill_number"],
            "title": vote_row["title"],
            "date": vote_row["date"],
            "description": vote_row["description"],
            "party_line": party_line,
            "member_vote": member_vote,
            "broke_rank": broke_rank,
            "unity_pct": unity
        })
    conn.close()
    return pd.DataFrame(rows)

# ----------------------- STREAMLIT APP -----------------------
page = st.sidebar.radio(
    "Navigate",
    ["Legislator Lookup", "Bill Lookup"]
)

if page == "Legislator Lookup":
    st.header("Legislator Voting Record")

    legislators = get_legislators()

    if legislators.empty:
        st.warning("No legislators found. Please run fetcher.py first.")
    else:
        chamber_filter = st.radio(
            "Filter by chamber",
            ["Both", "House", "Senate"],
            horizontal=True
        )
        filtered = legislators if chamber_filter == "Both" else legislators[legislators["chamber"] == chamber_filter]
        filtered = filtered.copy()
        filtered["label"] = filtered.apply(
            lambda row: f"{row['name']} ({row['party']}) - {row['chamber']} District {row['district']}",
            axis=1
        )
        selected_label = st.selectbox("Select a legislator", filtered["label"].tolist())
        selected_row = filtered[filtered["label"] == selected_label].iloc[0]

        people_id = int(selected_row["people_id"])
        name = selected_row["name"]
        party = selected_row["party"]
        chamber = selected_row["chamber"]
        district = selected_row["district"]

        st.subheader(f"Voting record for {name}")

        summary = get_vote_summary(people_id)
        record = get_voting_record(people_id)

        if not summary.empty:
            cols = st.columns(len(summary))
            for i, row in summary.iterrows():
                cols[i].metric(row["vote_text"], row["count"])

        if record.empty:
            st.info("No votes on record for this legislator yet.")
        else:
            record["Result"] = record["passed"].apply(lambda x: "✅ Passed" if x == 1 else "❌ Failed")
            display = record[["bill_number", "title", "date", "description", "Result", "vote_text"]].rename(columns={
                "bill_number": "Bill",
                "title": "Title",
                "date": "Date",
                "description": "Vote Description",
                "vote_text": "Vote Cast"
            })
            st.dataframe(display, use_container_width=True)

        st.subheader("Download Reports")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Generate Voting Record PDF"):
                with st.spinner("Generating Voting Record PDF..."):
                    pdf = generate_legislator_voting_record(name, party, chamber, district, summary, record)
                st.download_button(f"⬇️ Download {name} Voting Record", pdf, file_name=f"{name.replace(' ','_')}_voting_record.pdf", mime="application/pdf")

        with col2:
            if st.button("Generate Party Line Report PDF"):
                with st.spinner("Generating Party Line PDF..."):
                    party_line_df = build_legislator_party_line_df(people_id, party, record)
                    pdf = generate_legislator_party_line_report(name, party, chamber, district, party_line_df)
                st.download_button(f"⬇️ Download {name} Party Line", pdf, file_name=f"{name.replace(' ','_')}_party_line.pdf", mime="application/pdf")

        with col3:
            if st.button("Generate Both Reports PDF"):
                with st.spinner("Generating Both PDFs..."):
                    party_line_df = build_legislator_party_line_df(people_id, party, record)
                    pdf_vr = generate_legislator_voting_record(name, party, chamber, district, summary, record)
                    pdf_pl = generate_legislator_party_line_report(name, party, chamber, district, party_line_df)
                st.download_button(f"⬇️ Download Voting Record", pdf_vr, file_name=f"{name.replace(' ','_')}_voting_record.pdf", mime="application/pdf")
                st.download_button(f"⬇️ Download Party Line", pdf_pl, file_name=f"{name.replace(' ','_')}_party_line.pdf", mime="application/pdf")


def get_bill_search_filters():
    """Return distinct values for filter dropdowns."""
    conn = get_connection()
 
    sessions = pd.read_sql_query(
        "SELECT DISTINCT session FROM bills WHERE session != '' ORDER BY session DESC", conn
    )
    statuses = pd.read_sql_query(
        "SELECT DISTINCT status FROM bills WHERE status != '' ORDER BY status", conn
    )
    committees = pd.read_sql_query(
        "SELECT DISTINCT committee_name FROM committees WHERE committee_name != '' ORDER BY committee_name", conn
    )
    sponsors = pd.read_sql_query(
        "SELECT DISTINCT name FROM sponsors WHERE name != '' ORDER BY name", conn
    )
    conn.close()
    return (
        sessions["session"].tolist(),
        statuses["status"].tolist(),
        committees["committee_name"].tolist(),
        sponsors["name"].tolist(),
    )
 
def search_bills(bill_number_query, title_query, chamber_filter,
                 status_filter, session_filter, committee_filter, sponsor_filter):
    """Return bills matching all active filters."""
    conn = get_connection()
 
    sql = """
        SELECT DISTINCT
            b.bill_id,
            b.bill_number,
            b.title,
            b.session,
            b.status,
            b.chamber,
            b.url
        FROM bills b
        LEFT JOIN committees c ON b.bill_id = c.bill_id
        LEFT JOIN sponsors s   ON b.bill_id = s.bill_id
        WHERE 1=1
    """
    params = []
 
    if bill_number_query:
        sql += " AND b.bill_number LIKE ?"
        params.append(f"%{bill_number_query}%")
 
    if title_query:
        sql += " AND b.title LIKE ?"
        params.append(f"%{title_query}%")
 
    if chamber_filter and chamber_filter != "All":
        sql += " AND b.chamber = ?"
        params.append(chamber_filter)
 
    if status_filter and status_filter != "All":
        sql += " AND b.status = ?"
        params.append(status_filter)
 
    if session_filter and session_filter != "All":
        sql += " AND b.session = ?"
        params.append(session_filter)
 
    if committee_filter and committee_filter != "All":
        sql += " AND c.committee_name = ?"
        params.append(committee_filter)
 
    if sponsor_filter and sponsor_filter != "All":
        sql += " AND s.name = ?"
        params.append(sponsor_filter)
 
    sql += " ORDER BY b.bill_number"
 
    df = pd.read_sql_query(sql, conn, params=params)
    conn.close()
    return df
 
def get_bill_sponsors(bill_id):
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT name, sponsor_type
        FROM sponsors
        WHERE bill_id = ?
        ORDER BY sponsor_type, name
    """, conn, params=(bill_id,))
    conn.close()
    return df
 
def get_bill_committees(bill_id):
    conn = get_connection()
    df = pd.read_sql_query("""
        SELECT committee_name, chamber
        FROM committees
        WHERE bill_id = ?
    """, conn, params=(bill_id,))
    conn.close()
    return df
 
 
# ----------------------- BILL LOOKUP PAGE --------------------------
# Replace your existing "elif page == 'Bill Lookup':" block with this:
 
elif page == "Bill Lookup":
    st.header("Bill Search")
 
    # --- Load filter options ---
    sessions, statuses, committees, sponsors = get_bill_search_filters()
 
    # --- Search controls ---
    with st.expander("🔍 Search & Filter", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            bill_number_query = st.text_input("Bill Number", placeholder="e.g. HB 42")
            title_query = st.text_input("Keyword in Title", placeholder="e.g. education")
            chamber_filter = st.radio("Chamber", ["All", "House", "Senate"], horizontal=True)
        with col2:
            status_filter = st.selectbox("Status", ["All"] + statuses)
            session_filter = st.selectbox("Session", ["All"] + sessions)
            committee_filter = st.selectbox("Committee", ["All"] + committees)
            sponsor_filter = st.selectbox("Sponsor", ["All"] + sponsors)
 
    results = search_bills(
        bill_number_query, title_query, chamber_filter,
        status_filter, session_filter, committee_filter, sponsor_filter
    )
 
    st.caption(f"{len(results)} bill(s) found")
 
    if results.empty:
        st.info("No bills match your search. Try adjusting your filters.")
    else:
        # --- Results table ---
        display = results[["bill_number", "title", "chamber", "status", "session"]].rename(columns={
            "bill_number": "Bill",
            "title": "Title",
            "chamber": "Chamber",
            "status": "Status",
            "session": "Session",
        })
        st.dataframe(display, use_container_width=True)
 
        # --- Bill detail ---
        st.subheader("Bill Detail")
        result_labels = (results["bill_number"] + " — " + results["title"]).tolist()
        selected_label = st.selectbox("Select a bill to view details", result_labels)
        selected_idx = result_labels.index(selected_label)
        selected_bill = results.iloc[selected_idx]
 
        bill_id = int(selected_bill["bill_id"])
        bill_number = selected_bill["bill_number"]
        bill_title = selected_bill["title"]
 
        col1, col2, col3 = st.columns(3)
        col1.metric("Chamber", selected_bill["chamber"] or "—")
        col2.metric("Status", selected_bill["status"] or "—")
        col3.metric("Session", selected_bill["session"] or "—")
 
        if selected_bill["url"]:
            st.caption(f"[View on LegiScan]({selected_bill['url']})")
 
        # Sponsors
        sponsors_df = get_bill_sponsors(bill_id)
        if not sponsors_df.empty:
            primary = sponsors_df[sponsors_df["sponsor_type"] == "Primary"]["name"].tolist()
            cospon = sponsors_df[sponsors_df["sponsor_type"] == "Co-Sponsor"]["name"].tolist()
            if primary:
                st.markdown(f"**Primary Sponsor:** {', '.join(primary)}")
            if cospon:
                st.markdown(f"**Co-Sponsors:** {', '.join(cospon)}")
 
        # Committees
        committees_df = get_bill_committees(bill_id)
        if not committees_df.empty:
            committee_list = committees_df["committee_name"].tolist()
            st.markdown(f"**Committee:** {', '.join(committee_list)}")
 
        st.divider()
 
        # --- Roll call votes ---
        roll_calls = get_bill_votes(bill_id)
        if roll_calls.empty:
            st.info("No roll call votes recorded for this bill yet.")
        else:
            rc_list = []
            for _, rc in roll_calls.iterrows():
                detail = get_roll_call_detail(int(rc["roll_call_id"]))
                party_summary = calculate_party_line(detail) if not detail.empty else pd.DataFrame()
                rc_list.append({
                    "date": rc["date"],
                    "description": rc["description"],
                    "passed": rc["passed"],
                    "yea": rc["yea"],
                    "nay": rc["nay"],
                    "nv": rc["nv"],
                    "party_summary_df": party_summary,
                    "detail_df": detail
                })
 
            for rc in rc_list:
                result_label = "✅ Passed" if rc["passed"] == 1 else "❌ Failed"
                st.subheader(f"{rc['date']} — {rc['description']} ({result_label})")
                col1, col2, col3 = st.columns(3)
                col1.metric("Yea", rc["yea"])
                col2.metric("Nay", rc["nay"])
                col3.metric("NV/Absent", rc["nv"])
 
                if not rc["party_summary_df"].empty:
                    st.markdown("**Party Line Analysis**")
                    st.dataframe(rc["party_summary_df"], use_container_width=True)
 
                if not rc["detail_df"].empty:
                    with st.expander("See all individual votes"):
                        st.dataframe(
                            rc["detail_df"][["name", "party", "chamber", "district", "vote_text"]].rename(columns={
                                "name": "Name", "party": "Party", "chamber": "Chamber",
                                "district": "District", "vote_text": "Vote"
                            }),
                            use_container_width=True
                        )
                st.divider()
 
            # PDF downloads
            st.subheader("Download Reports")
            col1, col2, col3 = st.columns(3)
 
            with col1:
                if st.button("Generate Bill Vote Report PDF"):
                    with st.spinner("Generating Vote Report PDF..."):
                        pdf = generate_bill_vote_report(bill_number, bill_title, rc_list)
                    st.download_button(f"⬇️ Download {bill_number} Vote Report", pdf,
                                       file_name=f"{bill_number.replace(' ','_')}_vote_report.pdf",
                                       mime="application/pdf")
 
            with col2:
                if st.button("Generate Bill Party Line Report PDF"):
                    with st.spinner("Generating Bill Party Line PDF..."):
                        pdf = generate_bill_party_line_report(bill_number, bill_title, rc_list)
                    st.download_button(f"⬇️ Download {bill_number} Party Line", pdf,
                                       file_name=f"{bill_number.replace(' ','_')}_party_line.pdf",
                                       mime="application/pdf")
 
            with col3:
                if st.button("Generate Both Bill Reports PDF"):
                    with st.spinner("Generating Both Bill PDFs..."):
                        pdf_vote = generate_bill_vote_report(bill_number, bill_title, rc_list)
                        pdf_party = generate_bill_party_line_report(bill_number, bill_title, rc_list)
                    st.download_button(f"⬇️ Download Vote Report", pdf_vote,
                                       file_name=f"{bill_number.replace(' ','_')}_vote_report.pdf",
                                       mime="application/pdf")
                    st.download_button(f"⬇️ Download Party Line Report", pdf_party,
                                       file_name=f"{bill_number.replace(' ','_')}_party_line.pdf",
                                       mime="application/pdf")