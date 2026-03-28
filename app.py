import streamlit as st
import pandas as pd
import sqlite3
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

# ----------------------- DATA LOADING -----------------------
# Load everything into memory once when the app starts
# st.cache_resource means "load this once and keep it for all users"
# This is much faster than querying SQLite on every interaction

@st.cache_resource
def load_all_data():
    conn = sqlite3.connect("mo_votes.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row

    legislators = pd.read_sql_query(
        "SELECT people_id, name, party, chamber, district FROM legislators ORDER BY chamber, name",
        conn
    )
    bills = pd.read_sql_query(
        "SELECT bill_id, bill_number, title, session, status, chamber, url FROM bills ORDER BY bill_number",
        conn
    )
    votes = pd.read_sql_query(
        "SELECT roll_call_id, bill_id, date, description, chamber, yea, nay, nv, passed FROM votes",
        conn
    )
    member_votes = pd.read_sql_query(
        "SELECT roll_call_id, people_id, vote_text FROM member_votes",
        conn
    )

    try:
        sponsors = pd.read_sql_query(
            "SELECT bill_id, name, sponsor_type FROM sponsors", conn
        )
    except Exception:
        sponsors = pd.DataFrame(columns=["bill_id", "name", "sponsor_type"])

    try:
        committees = pd.read_sql_query(
            "SELECT bill_id, committee_name, chamber FROM committees", conn
        )
    except Exception:
        committees = pd.DataFrame(columns=["bill_id", "committee_name", "chamber"])

    # Build a merged legislator+vote table once — used everywhere
    member_votes_full = member_votes.merge(
        legislators[["people_id", "name", "party", "chamber", "district"]],
        on="people_id", how="left"
    )

    conn.close()
    return legislators, bills, votes, member_votes, member_votes_full, sponsors, committees

legislators, bills, votes, member_votes, member_votes_full, sponsors, committees = load_all_data()

# ----------------------- HELPER FUNCTIONS -----------------------
# These all operate on in-memory dataframes — no database queries

def get_voting_record(people_id):
    mv = member_votes_full[member_votes_full["people_id"] == people_id].copy()
    merged = mv.merge(votes, on="roll_call_id", how="left")
    merged = merged.merge(bills[["bill_id", "bill_number", "title", "url"]], on="bill_id", how="left")
    merged = merged.sort_values("date", ascending=False)
    return merged[["bill_number", "title", "date", "description", "passed", "vote_text", "url"]]

def get_vote_summary(people_id):
    mv = member_votes[member_votes["people_id"] == people_id]
    summary = mv.groupby("vote_text").size().reset_index(name="count")
    return summary.sort_values("count", ascending=False)

def get_roll_call_detail(roll_call_id):
    mv = member_votes_full[member_votes_full["roll_call_id"] == roll_call_id].copy()
    return mv[["name", "party", "chamber", "district", "vote_text"]].sort_values(["party", "name"])

def get_bill_votes(bill_id):
    return votes[votes["bill_id"] == bill_id].sort_values("date", ascending=False)

def get_bill_sponsors(bill_id):
    if sponsors.empty:
        return pd.DataFrame()
    return sponsors[sponsors["bill_id"] == bill_id]

def get_bill_committees(bill_id):
    if committees.empty:
        return pd.DataFrame()
    return committees[committees["bill_id"] == bill_id]

def search_bills(bill_number_query, title_query, chamber_filter,
                 status_filter, session_filter, committee_filter, sponsor_filter):
    result = bills.copy()
    if bill_number_query:
        result = result[result["bill_number"].str.contains(bill_number_query, case=False, na=False)]
    if title_query:
        result = result[result["title"].str.contains(title_query, case=False, na=False)]
    if chamber_filter and chamber_filter != "All":
        result = result[result["chamber"] == chamber_filter]
    if status_filter and status_filter != "All":
        result = result[result["status"] == status_filter]
    if session_filter and session_filter != "All":
        result = result[result["session"] == session_filter]
    if committee_filter and committee_filter != "All" and not committees.empty:
        matching_bill_ids = committees[
            committees["committee_name"] == committee_filter
        ]["bill_id"].tolist()
        result = result[result["bill_id"].isin(matching_bill_ids)]
    if sponsor_filter and sponsor_filter != "All" and not sponsors.empty:
        matching_bill_ids = sponsors[
            sponsors["name"] == sponsor_filter
        ]["bill_id"].tolist()
        result = result[result["bill_id"].isin(matching_bill_ids)]
    return result.head(200)

def calculate_party_line(roll_call_df):
    results = []
    for party in sorted(roll_call_df["party"].dropna().unique()):
        if party == "":
            continue
        pv = roll_call_df[roll_call_df["party"] == party]
        total = len(pv)
        yea = len(pv[pv["vote_text"] == "Yea"])
        nay = len(pv[pv["vote_text"] == "Nay"])
        nv = len(pv[pv["vote_text"].isin(["NV", "Absent"])])
        party_line = "Yea" if yea >= nay else "Nay"
        unity = round((max(yea, nay) / total) * 100, 1) if total > 0 else 0
        broke = pv[
            (pv["vote_text"].isin(["Yea", "Nay"])) &
            (pv["vote_text"] != party_line)
        ]["name"].tolist()
        results.append({
            "Party": party,
            "Total Members": total,
            "Yea": yea,
            "Nay": nay,
            "NV/Absent": nv,
            "Party Line": party_line,
            "Unity %": unity,
            "Broke Rank": ", ".join(broke) if broke else "None"
        })
    return pd.DataFrame(results)

def build_legislator_party_line_df(people_id, party, record_df):
    rows = []
    for _, vote_row in record_df.iterrows():
        # Find the roll_call_id for this specific vote
        match = votes[
            (votes["bill_id"].isin(bills[bills["bill_number"] == vote_row["bill_number"]]["bill_id"])) &
            (votes["date"] == vote_row["date"]) &
            (votes["description"] == vote_row["description"])
        ]
        if match.empty:
            continue
        roll_call_id = int(match.iloc[0]["roll_call_id"])
        detail = get_roll_call_detail(roll_call_id)
        if detail.empty:
            continue
        pv = detail[detail["party"] == party]
        if pv.empty:
            continue
        yea = len(pv[pv["vote_text"] == "Yea"])
        nay = len(pv[pv["vote_text"] == "Nay"])
        total = len(pv)
        party_line = "Yea" if yea >= nay else "Nay"
        unity = round((max(yea, nay) / total) * 100, 1) if total > 0 else 0
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
    return pd.DataFrame(rows)

# ----------------------- BILL DETAIL RENDERER -----------------------

def render_bill_detail(bill_id, bill_number, bill_title, bill_row):
    col1, col2, col3 = st.columns(3)
    col1.metric("Chamber", bill_row["chamber"] or "—")
    col2.metric("Status", bill_row["status"] or "—")
    col3.metric("Session", bill_row["session"] or "—")

    if bill_row["url"]:
        st.caption(f"[View on LegiScan]({bill_row['url']})")

    sp = get_bill_sponsors(bill_id)
    if not sp.empty:
        primary = sp[sp["sponsor_type"] == "Primary"]["name"].tolist()
        cospon = sp[sp["sponsor_type"] == "Co-Sponsor"]["name"].tolist()
        if primary:
            st.markdown(f"**Primary Sponsor:** {', '.join(primary)}")
        if cospon:
            st.markdown(f"**Co-Sponsors:** {', '.join(cospon)}")

    cm = get_bill_committees(bill_id)
    if not cm.empty:
        st.markdown(f"**Committee:** {', '.join(cm['committee_name'].tolist())}")

    st.divider()

    roll_calls = get_bill_votes(bill_id)
    if roll_calls.empty:
        st.info("No roll call votes recorded for this bill yet.")
        return

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
        with st.expander(f"{rc['date']} — {rc['description']} ({result_label})"):
            c1, c2, c3 = st.columns(3)
            c1.metric("Yea", rc["yea"])
            c2.metric("Nay", rc["nay"])
            c3.metric("NV/Absent", rc["nv"])

            if not rc["party_summary_df"].empty:
                st.markdown("**Party Line Analysis**")
                st.dataframe(rc["party_summary_df"], use_container_width=True)

            if not rc["detail_df"].empty:
                st.markdown("**Individual Votes**")
                st.dataframe(
                    rc["detail_df"].rename(columns={
                        "name": "Name", "party": "Party",
                        "chamber": "Chamber", "district": "District",
                        "vote_text": "Vote"
                    }),
                    use_container_width=True
                )

    st.subheader("Download Reports")
    c1, c2, c3 = st.columns(3)
    with c1:
        if st.button("Generate Bill Vote Report PDF", key=f"vote_pdf_{bill_id}"):
            with st.spinner("Generating PDF..."):
                pdf = generate_bill_vote_report(bill_number, bill_title, rc_list)
            st.download_button(
                f"⬇️ Download {bill_number} Vote Report", pdf,
                file_name=f"{bill_number.replace(' ', '_')}_vote_report.pdf",
                mime="application/pdf", key=f"dl_vote_{bill_id}"
            )
    with c2:
        if st.button("Generate Bill Party Line PDF", key=f"party_pdf_{bill_id}"):
            with st.spinner("Generating PDF..."):
                pdf = generate_bill_party_line_report(bill_number, bill_title, rc_list)
            st.download_button(
                f"⬇️ Download {bill_number} Party Line", pdf,
                file_name=f"{bill_number.replace(' ', '_')}_party_line.pdf",
                mime="application/pdf", key=f"dl_party_{bill_id}"
            )
    with c3:
        if st.button("Generate Both Bill PDFs", key=f"both_pdf_{bill_id}"):
            with st.spinner("Generating PDFs..."):
                pdf_vote = generate_bill_vote_report(bill_number, bill_title, rc_list)
                pdf_party = generate_bill_party_line_report(bill_number, bill_title, rc_list)
            st.download_button(
                f"⬇️ Download Vote Report", pdf_vote,
                file_name=f"{bill_number.replace(' ', '_')}_vote_report.pdf",
                mime="application/pdf", key=f"dl_both_vote_{bill_id}"
            )
            st.download_button(
                f"⬇️ Download Party Line Report", pdf_party,
                file_name=f"{bill_number.replace(' ', '_')}_party_line.pdf",
                mime="application/pdf", key=f"dl_both_party_{bill_id}"
            )

# ----------------------- SESSION STATE -----------------------

if "selected_bill_id" not in st.session_state:
    st.session_state.selected_bill_id = None
if "last_search" not in st.session_state:
    st.session_state.last_search = ()

# ----------------------- PAGE ROUTING -----------------------

page = st.sidebar.radio("Navigate", ["Legislator Lookup", "Bill Lookup"])

# ----------------------- LEGISLATOR LOOKUP -----------------------

if page == "Legislator Lookup":
    st.header("Legislator Voting Record")

    if legislators.empty:
        st.warning("No legislators found. Please run fetcher.py first.")
    else:
        chamber_filter = st.radio("Filter by chamber", ["Both", "House", "Senate"], horizontal=True)
        filtered = legislators if chamber_filter == "Both" else legislators[legislators["chamber"] == chamber_filter]
        filtered = filtered.copy()
        filtered["label"] = filtered.apply(
            lambda r: f"{r['name']} ({r['party']}) - {r['chamber']} District {r['district']}", axis=1
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
            st.dataframe(
                record[["bill_number", "title", "date", "description", "Result", "vote_text"]].rename(columns={
                    "bill_number": "Bill", "title": "Title", "date": "Date",
                    "description": "Vote Description", "vote_text": "Vote Cast"
                }),
                use_container_width=True
            )

        st.subheader("Download Reports")
        c1, c2, c3 = st.columns(3)

        with c1:
            if st.button("Generate Voting Record PDF"):
                with st.spinner("Generating PDF..."):
                    pdf = generate_legislator_voting_record(name, party, chamber, district, summary, record)
                st.download_button(
                    f"⬇️ Download {name} Voting Record", pdf,
                    file_name=f"{name.replace(' ', '_')}_voting_record.pdf",
                    mime="application/pdf"
                )
        with c2:
            if st.button("Generate Party Line Report PDF"):
                with st.spinner("Analyzing party votes..."):
                    pl_df = build_legislator_party_line_df(people_id, party, record)
                    pdf = generate_legislator_party_line_report(name, party, chamber, district, pl_df)
                st.download_button(
                    f"⬇️ Download {name} Party Line", pdf,
                    file_name=f"{name.replace(' ', '_')}_party_line.pdf",
                    mime="application/pdf"
                )
        with c3:
            if st.button("Generate Both Reports PDF"):
                with st.spinner("Generating both PDFs..."):
                    pl_df = build_legislator_party_line_df(people_id, party, record)
                    pdf_vr = generate_legislator_voting_record(name, party, chamber, district, summary, record)
                    pdf_pl = generate_legislator_party_line_report(name, party, chamber, district, pl_df)
                st.download_button(
                    f"⬇️ Download Voting Record", pdf_vr,
                    file_name=f"{name.replace(' ', '_')}_voting_record.pdf",
                    mime="application/pdf"
                )
                st.download_button(
                    f"⬇️ Download Party Line", pdf_pl,
                    file_name=f"{name.replace(' ', '_')}_party_line.pdf",
                    mime="application/pdf"
                )

# ----------------------- BILL LOOKUP -----------------------

elif page == "Bill Lookup":
    st.header("Bill Search")

    # Build filter options from in-memory data
    session_options = sorted(bills["session"].dropna().unique().tolist(), reverse=True)
    status_options = sorted(bills["status"].dropna().unique().tolist())
    committee_options = sorted(committees["committee_name"].dropna().unique().tolist()) if not committees.empty else []
    sponsor_options = sorted(sponsors["name"].dropna().unique().tolist()) if not sponsors.empty else []

    with st.expander("Search & Filter", expanded=True):
        col1, col2 = st.columns(2)
        with col1:
            bill_number_query = st.text_input("Bill Number", placeholder="e.g. HB 42")
            title_query = st.text_input("Keyword in Title", placeholder="e.g. education")
            chamber_filter = st.radio("Chamber", ["All", "House", "Senate"], horizontal=True)
        with col2:
            status_filter = st.selectbox("Status", ["All"] + status_options)
            session_filter = st.selectbox("Session", ["All"] + session_options)
            committee_filter = st.selectbox("Committee", ["All"] + committee_options)
            sponsor_filter = st.selectbox("Sponsor", ["All"] + sponsor_options)

    current_search = (
        bill_number_query, title_query, chamber_filter,
        status_filter, session_filter, committee_filter, sponsor_filter
    )
    if current_search != st.session_state.last_search:
        st.session_state.selected_bill_id = None
        st.session_state.last_search = current_search

    results = search_bills(
        bill_number_query, title_query, chamber_filter,
        status_filter, session_filter, committee_filter, sponsor_filter
    )

    st.caption(f"{len(results)} bill(s) found")

    if results.empty:
        st.info("No bills match your search. Try adjusting your filters.")
    else:
        st.markdown("#### Results")
        header_cols = st.columns([1, 3, 1, 1, 2, 1])
        for col, label in zip(header_cols, ["**Bill**", "**Title**", "**Chamber**", "**Status**", "**Session**", ""]):
            col.markdown(label)
        st.divider()

        for _, row in results.iterrows():
            cols = st.columns([1, 3, 1, 1, 2, 1])
            cols[0].write(row["bill_number"])
            cols[1].write(row["title"])
            cols[2].write(row["chamber"] or "—")
            cols[3].write(row["status"] or "—")
            cols[4].write(row["session"] or "—")
            if cols[5].button("View", key=f"view_{row['bill_id']}"):
                st.session_state.selected_bill_id = int(row["bill_id"])
                st.rerun()

        if st.session_state.selected_bill_id:
            matched = bills[bills["bill_id"] == st.session_state.selected_bill_id]
            if not matched.empty:
                bill_row = matched.iloc[0]
                st.divider()
                st.subheader(f"📄 {bill_row['bill_number']} — {bill_row['title']}")
                if st.button("← Back to results"):
                    st.session_state.selected_bill_id = None
                    st.rerun()
                render_bill_detail(
                    int(bill_row["bill_id"]),
                    bill_row["bill_number"],
                    bill_row["title"],
                    bill_row
                )