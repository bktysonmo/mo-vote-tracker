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
        sponsors = pd.read_sql_query("SELECT bill_id, name, sponsor_type FROM sponsors", conn)
    except Exception:
        sponsors = pd.DataFrame(columns=["bill_id", "name", "sponsor_type"])
    try:
        committees = pd.read_sql_query("SELECT bill_id, committee_name, chamber FROM committees", conn)
    except Exception:
        committees = pd.DataFrame(columns=["bill_id", "committee_name", "chamber"])

    member_votes_full = member_votes.merge(
        legislators[["people_id", "name", "party", "chamber", "district"]],
        on="people_id", how="left"
    )
    conn.close()
    return legislators, bills, votes, member_votes, member_votes_full, sponsors, committees

legislators, bills, votes, member_votes, member_votes_full, sponsors, committees = load_all_data()

@st.cache_resource
def get_history_db():
    try:
        conn = sqlite3.connect("mo_history.db", check_same_thread=False)
        conn.row_factory = sqlite3.Row
        return conn
    except Exception:
        return None

@st.cache_resource
def get_current_db():
    conn = sqlite3.connect("mo_votes.db", check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn

@st.cache_data
def load_history_lookups():
    try:
        conn = sqlite3.connect("mo_history.db", check_same_thread=False)
        h_sessions = pd.read_sql_query(
            "SELECT session_id, session_name, year, special FROM sessions ORDER BY year DESC, special ASC",
            conn
        )
        h_legislators = pd.read_sql_query("""
            SELECT people_id, name, party, chamber,
                   MAX(currently_serving) as currently_serving
            FROM legislators
            GROUP BY people_id
            ORDER BY name
        """, conn)
        h_sponsors = pd.read_sql_query(
            "SELECT bill_id, session_id, name, sponsor_type, people_id FROM sponsors", conn
        )
        h_similar = pd.read_sql_query(
            "SELECT bill_id, similar_bill_id, similar_number, similar_title, similar_session FROM similar_bills",
            conn
        )
        conn.close()
        return h_sessions, h_legislators, h_sponsors, h_similar
    except Exception as e:
        st.error(f"Could not load historical database: {e}")
        return pd.DataFrame(), pd.DataFrame(), pd.DataFrame(), pd.DataFrame()

h_sessions, h_legislators, h_sponsors, h_similar = load_history_lookups()

# ----------------------- CURRENT DB QUERIES -----------------------

def get_official_similar_bills(bill_id):
    conn = get_current_db()
    try:
        df = pd.read_sql_query("""
            SELECT similar_number, similar_year, relationship, source_version
            FROM similar_bills
            WHERE bill_id = ?
            ORDER BY similar_year DESC
        """, conn, params=(bill_id,))
        return df
    except Exception:
        return pd.DataFrame()

def get_bill_summary_text(bill_id):
    conn = get_current_db()
    try:
        df = pd.read_sql_query("""
            SELECT summary_text, source_version
            FROM bill_summaries
            WHERE bill_id = ?
        """, conn, params=(bill_id,))
        return df.iloc[0] if not df.empty else None
    except Exception:
        return None

# ----------------------- HISTORY DB QUERIES -----------------------

def search_history_bills(keyword, bill_num, year, session_type, status, session_options):
    hconn = get_history_db()
    if hconn is None:
        return pd.DataFrame()
    sql = """
        SELECT b.bill_id, b.session_id, b.session_name, b.bill_number,
               b.title, b.status, b.chamber, b.url
        FROM bills b
        JOIN sessions s ON b.session_id = s.session_id
        WHERE 1=1
    """
    params = []
    if keyword:
        sql += " AND b.title LIKE ?"
        params.append(f"%{keyword}%")
    if bill_num:
        sql += " AND b.bill_number LIKE ?"
        params.append(f"%{bill_num}%")
    if year != "All":
        sql += " AND s.year = ?"
        params.append(int(year))
    if session_type == "Regular only":
        sql += " AND s.special = 0"
    elif session_type == "Special only":
        sql += " AND s.special = 1"
    if status != "All":
        sql += " AND b.status = ?"
        params.append(status)
    sql += " ORDER BY s.year DESC, b.bill_number LIMIT 300"
    return pd.read_sql_query(sql, hconn, params=params if params else None)

def get_history_bill_votes(bill_id):
    hconn = get_history_db()
    if hconn is None:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT roll_call_id, bill_id, date, description, yea, nay, nv, passed, chamber
        FROM votes WHERE bill_id = ?
        ORDER BY date DESC
    """, hconn, params=(bill_id,))

def get_history_roll_call_detail(roll_call_id):
    hconn = get_history_db()
    if hconn is None:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT l.name, l.party, l.chamber, mv.vote_text,
               MAX(l.currently_serving) as currently_serving
        FROM member_votes mv
        JOIN legislators l ON mv.people_id = l.people_id
        WHERE mv.roll_call_id = ?
        GROUP BY mv.people_id
        ORDER BY l.party, l.name
    """, hconn, params=(roll_call_id,))

def get_history_legislator_votes(people_id):
    hconn = get_history_db()
    if hconn is None:
        return pd.DataFrame()
    return pd.read_sql_query("""
        SELECT mv.vote_text, mv.session_id,
               v.roll_call_id, v.bill_id, v.date, v.description,
               b.bill_number, b.title, b.session_name
        FROM member_votes mv
        JOIN votes v ON mv.roll_call_id = v.roll_call_id
        JOIN bills b ON v.bill_id = b.bill_id
        WHERE mv.people_id = ?
        ORDER BY v.date DESC
    """, hconn, params=(people_id,))

def get_history_topic_breakers(topic):
    hconn = get_history_db()
    if hconn is None:
        return pd.DataFrame(), pd.DataFrame()
    topic_bills = pd.read_sql_query("""
        SELECT bill_id, bill_number, title, session_name
        FROM bills WHERE title LIKE ?
        ORDER BY session_name DESC
    """, hconn, params=(f"%{topic}%",))
    if topic_bills.empty:
        return pd.DataFrame(), pd.DataFrame()
    bill_ids = topic_bills["bill_id"].tolist()
    placeholders = ",".join("?" * len(bill_ids))
    roll_calls = pd.read_sql_query(f"""
        SELECT roll_call_id, bill_id, date, passed
        FROM votes WHERE bill_id IN ({placeholders})
    """, hconn, params=bill_ids)
    if roll_calls.empty:
        return topic_bills, pd.DataFrame()
    rc_ids = roll_calls["roll_call_id"].tolist()
    placeholders2 = ",".join("?" * len(rc_ids))
    mv = pd.read_sql_query(f"""
        SELECT mv.roll_call_id, mv.people_id, mv.vote_text,
               l.name, l.party, MAX(l.currently_serving) as currently_serving
        FROM member_votes mv
        JOIN legislators l ON mv.people_id = l.people_id
        WHERE mv.roll_call_id IN ({placeholders2})
        GROUP BY mv.roll_call_id, mv.people_id
    """, hconn, params=rc_ids)
    return topic_bills, mv.merge(roll_calls, on="roll_call_id", how="left") \
                          .merge(topic_bills[["bill_id", "bill_number", "title", "session_name"]], on="bill_id", how="left")

# ----------------------- IN-MEMORY HELPERS -----------------------

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
        matching = committees[committees["committee_name"] == committee_filter]["bill_id"].tolist()
        result = result[result["bill_id"].isin(matching)]
    if sponsor_filter and sponsor_filter != "All" and not sponsors.empty:
        matching = sponsors[sponsors["name"] == sponsor_filter]["bill_id"].tolist()
        result = result[result["bill_id"].isin(matching)]
    return result.head(200)

def find_similar_bills(bill_title, current_bill_id, current_session_name, limit=10):
    hconn = get_history_db()
    if hconn is None:
        return pd.DataFrame()
    filler = {
        "the", "a", "an", "to", "of", "in", "for", "and", "or", "by",
        "with", "relating", "regarding", "concerning", "provides", "provide",
        "establishes", "establish", "modifies", "modify", "amends", "amend",
        "creates", "create", "act", "law", "section", "sections", "chapter",
        "this", "that", "which", "any", "all", "such", "other", "certain",
        "provisions", "provision", "relative", "makes", "make", "from",
        "requires", "require", "authorizes", "authorize", "allowing", "allow"
    }
    words = [
        w.lower().strip(".,;:()")
        for w in bill_title.split()
        if w.lower().strip(".,;:()") not in filler
        and len(w.strip(".,;:()")) > 3
    ]
    if not words:
        return pd.DataFrame()
    score_parts = " + ".join([
        f"CASE WHEN LOWER(b.title) LIKE ? THEN 1 ELSE 0 END"
        for _ in words
    ])
    params = [f"%{w}%" for w in words]
    sql = f"""
        SELECT b.bill_id, b.bill_number, b.title, b.session_name,
               b.status, b.chamber, b.url,
               ({score_parts}) as score
        FROM bills b
        WHERE b.bill_id != ?
        AND b.session_name != ?
        AND ({score_parts}) >= ?
        ORDER BY score DESC, b.session_name DESC
        LIMIT ?
    """
    min_score = max(2, len(words) // 3)
    full_params = params + [current_bill_id, current_session_name] + params + [min_score, limit]
    try:
        result = pd.read_sql_query(sql, hconn, params=full_params)
        return result[result["score"] > 0].copy()
    except Exception:
        return pd.DataFrame()

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

# ----------------------- SIMILAR BILL DETAIL HELPER -----------------------

def render_similar_bill_detail(sim_bill_id, sim_bill_number, sim_title, sim_session, sim_url, key_prefix):
    st.subheader(f"📄 {sim_bill_number} — {sim_title}")
    st.caption(f"{sim_session}")
    if sim_url:
        st.caption(f"[View on LegiScan]({sim_url})")
    sim_votes = get_history_bill_votes(sim_bill_id)
    if sim_votes.empty:
        st.info("No recorded roll call votes for this bill.")
    else:
        for _, rc in sim_votes.iterrows():
            result_label = "✅ Passed" if rc["passed"] == 1 else "❌ Failed"
            with st.expander(f"{rc['date']} — {rc['description']} ({result_label})"):
                c1, c2, c3 = st.columns(3)
                c1.metric("Yea", rc["yea"])
                c2.metric("Nay", rc["nay"])
                c3.metric("NV/Absent", rc["nv"])
                detail = get_history_roll_call_detail(int(rc["roll_call_id"]))
                if not detail.empty:
                    party_summary = calculate_party_line(detail)
                    if not party_summary.empty:
                        st.markdown("**Party Line Analysis**")
                        st.dataframe(party_summary, use_container_width=True)
                    st.markdown("**Individual Votes**")
                    st.dataframe(
                        detail[["name", "party", "chamber", "vote_text"]].rename(columns={
                            "name": "Name", "party": "Party",
                            "chamber": "Chamber", "vote_text": "Vote"
                        }),
                        use_container_width=True
                    )

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

    # Official bill summary
    summary_row = get_bill_summary_text(bill_id)
    if summary_row is not None:
        with st.expander("📝 Official Bill Summary"):
            version = summary_row["source_version"]
            source = "MO House PDF" if version in ["I", "S", "T", "C"] else "MO Senate website"
            st.caption(f"Source: {source} (version {version})")
            st.write(summary_row["summary_text"])

    # Similar bills
    with st.expander("📋 Similar Bills in Other Sessions"):
        official = get_official_similar_bills(bill_id)
        if not official.empty:
            st.caption("Official references from MO House/Senate summaries")
            for _, row in official.iterrows():
                rel = row["relationship"].replace("is ", "").title()
                st.markdown(f"- **{row['similar_number']}** ({row['similar_year']}) — {rel}")
            st.divider()

        with st.spinner("Searching historical sessions..."):
            similar = find_similar_bills(
                bill_title, bill_id,
                bill_row.get("session", "") or "",
                limit=10
            )
        if not similar.empty:
            st.caption("Related bills found by title matching in historical database")
            sim_display = similar[[
                "bill_number", "title", "session_name", "status", "score"
            ]].rename(columns={
                "bill_number": "Bill", "title": "Title",
                "session_name": "Session", "status": "Status", "score": "Relevance"
            }).reset_index(drop=True)

            sim_sel = st.dataframe(
                sim_display, use_container_width=True,
                on_select="rerun", selection_mode="single-row",
                key=f"sim_{bill_id}"
            )
            if sim_sel.selection.rows:
                sim_row = similar.iloc[sim_sel.selection.rows[0]]
                st.divider()
                render_similar_bill_detail(
                    int(sim_row["bill_id"]),
                    sim_row["bill_number"],
                    sim_row["title"],
                    sim_row["session_name"],
                    sim_row["url"],
                    key_prefix=f"cur_{bill_id}"
                )
        elif official.empty:
            st.info("No similar bills found.")

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

page = st.sidebar.radio("Navigate", ["Legislator Lookup", "Bill Lookup", "Historical Search"])

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

    results = search_bills(
        bill_number_query, title_query, chamber_filter,
        status_filter, session_filter, committee_filter, sponsor_filter
    )

    st.caption(f"{len(results)} bill(s) found — click any row to view details")

    if results.empty:
        st.info("No bills match your search. Try adjusting your filters.")
    else:
        display_results = results[["bill_number", "title", "chamber", "status", "session"]].rename(columns={
            "bill_number": "Bill", "title": "Title",
            "chamber": "Chamber", "status": "Status", "session": "Session"
        }).reset_index(drop=True)

        selected = st.dataframe(
            display_results, use_container_width=True,
            on_select="rerun", selection_mode="single-row"
        )

        if selected.selection.rows:
            row_index = selected.selection.rows[0]
            selected_bill = results.iloc[row_index]
            st.divider()
            st.subheader(f"📄 {selected_bill['bill_number']} — {selected_bill['title']}")
            render_bill_detail(
                int(selected_bill["bill_id"]),
                selected_bill["bill_number"],
                selected_bill["title"],
                selected_bill
            )

# ----------------------- HISTORICAL SEARCH -----------------------

elif page == "Historical Search":
    st.header("Historical Legislative Search")
    st.caption("Searching Missouri legislative sessions from 2015 to present")

    hist_tab1, hist_tab2, hist_tab3 = st.tabs([
        "Bill Search", "Legislator History", "Party Rank Patterns"
    ])

    # ---- HISTORICAL BILL SEARCH ----
    with hist_tab1:
        st.subheader("Search Bills Across All Sessions")

        col1, col2 = st.columns(2)
        with col1:
            h_keyword = st.text_input("Keyword in title", placeholder="e.g. education, tax, abortion")
            h_bill_num = st.text_input("Bill number", placeholder="e.g. HB 42")
        with col2:
            h_year = st.selectbox(
                "Year",
                ["All"] + sorted(h_sessions["year"].unique().tolist(), reverse=True)
            )
            h_session_type = st.radio(
                "Session type", ["All", "Regular only", "Special only"], horizontal=True
            )
            h_status = st.selectbox("Status", ["All", "Introduced", "Engrossed",
                                                "Enrolled", "Passed", "Vetoed", "Failed"])

        h_results = search_history_bills(
            h_keyword, h_bill_num, h_year, h_session_type, h_status, h_sessions
        )

        st.caption(f"{len(h_results)} bill(s) found")

        if not h_results.empty:
            display = h_results[[
                "bill_number", "title", "session_name", "status", "chamber"
            ]].rename(columns={
                "bill_number": "Bill", "title": "Title",
                "session_name": "Session", "status": "Status", "chamber": "Chamber"
            }).reset_index(drop=True)

            h_selected = st.dataframe(
                display, use_container_width=True,
                on_select="rerun", selection_mode="single-row"
            )

            if h_selected.selection.rows:
                row_index = h_selected.selection.rows[0]
                sel_bill = h_results.iloc[row_index]
                bill_id = int(sel_bill["bill_id"])

                st.divider()
                st.subheader(f"📄 {sel_bill['bill_number']} — {sel_bill['title']}")
                st.caption(f"{sel_bill['session_name']} | {sel_bill['status']} | {sel_bill['chamber']}")

                if sel_bill["url"]:
                    st.caption(f"[View on LegiScan]({sel_bill['url']})")

                bill_sponsors = h_sponsors[h_sponsors["bill_id"] == bill_id]
                if not bill_sponsors.empty:
                    primary = bill_sponsors[bill_sponsors["sponsor_type"] == "Primary"]["name"].tolist()
                    cospon = bill_sponsors[bill_sponsors["sponsor_type"] == "Co-Sponsor"]["name"].tolist()
                    if primary:
                        st.markdown(f"**Primary Sponsor:** {', '.join(primary)}")
                    if cospon:
                        st.markdown(f"**Co-Sponsors:** {', '.join(cospon)}")

                # Similar bills via title matching
                with st.expander("📋 Similar Bills in Other Sessions"):
                    with st.spinner("Searching..."):
                        h_similar_results = find_similar_bills(
                            sel_bill["title"], bill_id, sel_bill["session_name"], limit=10
                        )
                    if h_similar_results.empty:
                        st.info("No similar bills found in other sessions.")
                    else:
                        sim_display = h_similar_results[[
                            "bill_number", "title", "session_name", "status", "score"
                        ]].rename(columns={
                            "bill_number": "Bill", "title": "Title",
                            "session_name": "Session", "status": "Status", "score": "Relevance"
                        }).reset_index(drop=True)

                        h_sim_sel = st.dataframe(
                            sim_display, use_container_width=True,
                            on_select="rerun", selection_mode="single-row",
                            key=f"hsim_{bill_id}"
                        )
                        if h_sim_sel.selection.rows:
                            sim_row = h_similar_results.iloc[h_sim_sel.selection.rows[0]]
                            st.divider()
                            render_similar_bill_detail(
                                int(sim_row["bill_id"]),
                                sim_row["bill_number"],
                                sim_row["title"],
                                sim_row["session_name"],
                                sim_row["url"],
                                key_prefix=f"hist_{bill_id}"
                            )

                bill_votes = get_history_bill_votes(bill_id)
                if bill_votes.empty:
                    st.info("No recorded roll call votes for this bill.")
                else:
                    for _, rc in bill_votes.iterrows():
                        result = "✅ Passed" if rc["passed"] == 1 else "❌ Failed"
                        with st.expander(f"{rc['date']} — {rc['description']} ({result})"):
                            c1, c2, c3 = st.columns(3)
                            c1.metric("Yea", rc["yea"])
                            c2.metric("Nay", rc["nay"])
                            c3.metric("NV/Absent", rc["nv"])
                            detail = get_history_roll_call_detail(int(rc["roll_call_id"]))
                            if not detail.empty:
                                party_summary = calculate_party_line(detail)
                                if not party_summary.empty:
                                    st.markdown("**Party Line Analysis**")
                                    st.dataframe(party_summary, use_container_width=True)
                                st.markdown("**Individual Votes**")
                                st.dataframe(
                                    detail[["name", "party", "chamber", "vote_text"]].rename(columns={
                                        "name": "Name", "party": "Party",
                                        "chamber": "Chamber", "vote_text": "Vote"
                                    }),
                                    use_container_width=True
                                )

    # ---- LEGISLATOR HISTORY ----
    with hist_tab2:
        st.subheader("Legislator Voting History Across Sessions")

        serving_filter = st.radio(
            "Show",
            ["All legislators", "Currently serving only", "Former legislators only"],
            horizontal=True
        )

        if serving_filter == "Currently serving only":
            leg_pool = h_legislators[h_legislators["currently_serving"] == 1]
        elif serving_filter == "Former legislators only":
            leg_pool = h_legislators[h_legislators["currently_serving"] == 0]
        else:
            leg_pool = h_legislators

        leg_pool = leg_pool.copy().sort_values("name")
        leg_pool["label"] = leg_pool.apply(
            lambda r: f"{r['name']} ({r['party']}) — {'Currently Serving' if r['currently_serving'] == 1 else 'Former'}",
            axis=1
        )

        selected_leg = st.selectbox("Select a legislator", leg_pool["label"].tolist())
        sel_leg_row = leg_pool[leg_pool["label"] == selected_leg].iloc[0]
        sel_people_id = int(sel_leg_row["people_id"])

        if st.button("Load Voting History"):
            with st.spinner("Loading voting history..."):
                leg_votes = get_history_legislator_votes(sel_people_id)

            if leg_votes.empty:
                st.info("No voting history found for this legislator.")
            else:
                st.markdown("#### Sessions Served")
                session_summary = leg_votes.groupby("session_name").agg(
                    total_votes=("vote_text", "count"),
                    yea=("vote_text", lambda x: (x == "Yea").sum()),
                    nay=("vote_text", lambda x: (x == "Nay").sum()),
                    nv=("vote_text", lambda x: x.isin(["NV", "Absent"]).sum())
                ).reset_index().rename(columns={
                    "session_name": "Session",
                    "total_votes": "Total Votes",
                    "yea": "Yea", "nay": "Nay", "nv": "NV/Absent"
                })
                st.dataframe(session_summary, use_container_width=True)

                st.markdown("#### Full Vote History")
                full_display = leg_votes[[
                    "bill_number", "title", "session_name", "date", "description", "vote_text"
                ]].rename(columns={
                    "bill_number": "Bill", "title": "Title",
                    "session_name": "Session", "date": "Date",
                    "description": "Description", "vote_text": "Vote"
                }).reset_index(drop=True)
                st.dataframe(full_display, use_container_width=True)

    # ---- PARTY RANK PATTERNS ----
    with hist_tab3:
        st.subheader("Cross-Session Party Rank Patterns")
        st.caption("Find legislators with a pattern of breaking party rank across multiple sessions")

        pr_mode = st.radio("Analyze by", ["Legislator", "Topic keyword"], horizontal=True)

        if pr_mode == "Legislator":
            serving_filter2 = st.radio(
                "Show", ["All", "Currently serving", "Former"],
                horizontal=True, key="pr_serving"
            )
            if serving_filter2 == "Currently serving":
                pr_leg_pool = h_legislators[h_legislators["currently_serving"] == 1]
            elif serving_filter2 == "Former":
                pr_leg_pool = h_legislators[h_legislators["currently_serving"] == 0]
            else:
                pr_leg_pool = h_legislators

            pr_leg_pool = pr_leg_pool.copy().sort_values("name")
            pr_leg_pool["label"] = pr_leg_pool.apply(
                lambda r: f"{r['name']} ({r['party']}) — {'Currently Serving' if r['currently_serving'] == 1 else 'Former'}",
                axis=1
            )

            pr_selected = st.selectbox("Select a legislator", pr_leg_pool["label"].tolist(), key="pr_leg")
            pr_sel_row = pr_leg_pool[pr_leg_pool["label"] == pr_selected].iloc[0]
            pr_people_id = int(pr_sel_row["people_id"])
            pr_party = pr_sel_row["party"]

            if st.button("Calculate Party Rank Pattern"):
                with st.spinner("Analyzing voting history across all sessions..."):
                    pr_votes = get_history_legislator_votes(pr_people_id)

                if pr_votes.empty:
                    st.info("No voting history found.")
                else:
                    rows = []
                    for _, vote_row in pr_votes.iterrows():
                        rc_id = int(vote_row["roll_call_id"])
                        all_rc = get_history_roll_call_detail(rc_id)
                        if all_rc.empty:
                            continue
                        pv = all_rc[all_rc["party"] == pr_party]
                        if pv.empty:
                            continue
                        yea = (pv["vote_text"] == "Yea").sum()
                        nay = (pv["vote_text"] == "Nay").sum()
                        total = len(pv)
                        party_line = "Yea" if yea >= nay else "Nay"
                        unity = round((max(yea, nay) / total) * 100, 1) if total > 0 else 0
                        member_vote = vote_row["vote_text"]
                        broke = member_vote in ["Yea", "Nay"] and member_vote != party_line
                        rows.append({
                            "Session": vote_row.get("session_name", ""),
                            "Bill": vote_row.get("bill_number", ""),
                            "Title": vote_row.get("title", ""),
                            "Party Line": party_line,
                            "Member Vote": member_vote,
                            "Broke Rank": "YES" if broke else "No",
                            "Party Unity %": unity
                        })

                    if not rows:
                        st.info("Could not calculate party line data.")
                    else:
                        pattern_df = pd.DataFrame(rows)
                        broke_df = pattern_df[pattern_df["Broke Rank"] == "YES"]
                        total_rc = len(pattern_df[pattern_df["Member Vote"].isin(["Yea", "Nay"])])
                        total_broke = len(broke_df)
                        unity_score = round(((total_rc - total_broke) / total_rc) * 100, 1) if total_rc > 0 else 0

                        c1, c2, c3 = st.columns(3)
                        c1.metric("Overall Party Unity Score", f"{unity_score}%")
                        c2.metric("Times Broke Rank", total_broke)
                        c3.metric("Total Votes Analyzed", total_rc)

                        serving_label = "Currently Serving" if pr_sel_row["currently_serving"] == 1 else "Former Legislator"
                        st.caption(f"{pr_sel_row['name']} | {pr_party} | {serving_label}")

                        if not broke_df.empty:
                            st.markdown("#### Votes Where Member Broke Party Rank")
                            st.dataframe(
                                broke_df[["Session", "Bill", "Title", "Party Line", "Member Vote", "Party Unity %"]],
                                use_container_width=True
                            )

                        st.markdown("#### Full Cross-Session Vote History")
                        st.dataframe(pattern_df, use_container_width=True)

        elif pr_mode == "Topic keyword":
            topic = st.text_input("Enter a topic keyword", placeholder="e.g. abortion, tax, guns, education")

            if topic and st.button("Find Party Rank Breakers"):
                with st.spinner(f"Searching '{topic}' votes across all sessions..."):
                    topic_bills, mv_data = get_history_topic_breakers(topic)

                if topic_bills.empty:
                    st.info(f"No bills found matching '{topic}'.")
                elif mv_data.empty:
                    st.info(f"No roll call votes found for '{topic}' bills.")
                else:
                    breaker_rows = []
                    for rc_id in mv_data["roll_call_id"].unique():
                        rc_votes = mv_data[mv_data["roll_call_id"] == rc_id]
                        bill_info = rc_votes.iloc[0]
                        for party in rc_votes["party"].dropna().unique():
                            if party == "":
                                continue
                            pv = rc_votes[rc_votes["party"] == party]
                            yea = (pv["vote_text"] == "Yea").sum()
                            nay = (pv["vote_text"] == "Nay").sum()
                            total = len(pv)
                            if total == 0:
                                continue
                            party_line = "Yea" if yea >= nay else "Nay"
                            unity = round((max(yea, nay) / total) * 100, 1)
                            breakers = pv[
                                (pv["vote_text"].isin(["Yea", "Nay"])) &
                                (pv["vote_text"] != party_line)
                            ]
                            for _, br in breakers.iterrows():
                                breaker_rows.append({
                                    "Session": bill_info.get("session_name", ""),
                                    "Bill": bill_info.get("bill_number", ""),
                                    "Title": bill_info.get("title", ""),
                                    "Legislator": br["name"],
                                    "Party": party,
                                    "Status": "Currently Serving" if br.get("currently_serving") == 1 else "Former",
                                    "Party Line": party_line,
                                    "Their Vote": br["vote_text"],
                                    "Party Unity %": unity
                                })

                    if not breaker_rows:
                        st.info("No party rank breaks found for this topic.")
                    else:
                        breaker_df = pd.DataFrame(breaker_rows)

                        # Summary table with All row at top
                        repeat_breakers = breaker_df.groupby(
                            ["Legislator", "Party", "Status"]
                        ).size().reset_index(name="Times Broke Rank") \
                         .sort_values("Times Broke Rank", ascending=False)

                        all_row = pd.DataFrame([{
                            "Legislator": "⬛ All Legislators",
                            "Party": "",
                            "Status": "",
                            "Times Broke Rank": len(breaker_df)
                        }])
                        summary_table = pd.concat([all_row, repeat_breakers], ignore_index=True)

                        st.markdown(f"#### Party Rank Breakers on '{topic}' Bills")
                        st.caption(f"Click a row to see that legislator's specific breaking votes below")

                        sel = st.dataframe(
                            summary_table, use_container_width=True,
                            on_select="rerun", selection_mode="single-row",
                            key="breaker_summary"
                        )

                        selected_idx = sel.selection.rows[0] if sel.selection.rows else 0

                        st.divider()

                        if selected_idx == 0:
                            st.markdown("**All Rank-Breaking Votes**")
                            st.dataframe(
                                breaker_df[[
                                    "Legislator", "Party", "Status", "Session",
                                    "Bill", "Title", "Party Line", "Their Vote", "Party Unity %"
                                ]].sort_values(["Legislator", "Session"]),
                                use_container_width=True
                            )
                        else:
                            sel_name = summary_table.iloc[selected_idx]["Legislator"]
                            sel_party = summary_table.iloc[selected_idx]["Party"]
                            sel_status = summary_table.iloc[selected_idx]["Status"]
                            leg_breaks = breaker_df[breaker_df["Legislator"] == sel_name]

                            st.markdown(f"**{sel_name}** ({sel_party}) — {sel_status}")
                            st.caption(f"Broke party rank {len(leg_breaks)} time(s) on '{topic}' bills")

                            st.dataframe(
                                leg_breaks[[
                                    "Session", "Bill", "Title",
                                    "Party Line", "Their Vote", "Party Unity %"
                                ]].sort_values("Session"),
                                use_container_width=True
                            )