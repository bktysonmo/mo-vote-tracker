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


elif page == "Bill Lookup":
    st.header("Bill Vote Breakdown")

    bills = get_bills_with_votes()
    if bills.empty:
        st.warning("No bills with votes found. Please run fetcher.py first.")
    else:
        bills["label"] = bills["bill_number"] + " — " + bills["title"]
        selected_label = st.selectbox("Select a bill", bills["label"].tolist())
        selected_bill = bills[bills["label"] == selected_label].iloc[0]

        bill_id = int(selected_bill["bill_id"])
        bill_number = selected_bill["bill_number"]
        bill_title = selected_bill["title"]

        if selected_bill["url"]:
            st.caption(f"[View on LegiScan]({selected_bill['url']})")

        roll_calls = get_bill_votes(bill_id)
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

        # Display votes
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
                        rc["detail_df"][["name","party","chamber","district","vote_text"]].rename(columns={
                            "name":"Name","party":"Party","chamber":"Chamber","district":"District","vote_text":"Vote"
                        }),
                        use_container_width=True
                    )
            st.divider()

        # PDF download buttons
        st.subheader("Download Reports")
        col1, col2, col3 = st.columns(3)

        with col1:
            if st.button("Generate Bill Vote Report PDF"):
                with st.spinner("Generating Vote Report PDF..."):
                    pdf = generate_bill_vote_report(bill_number, bill_title, rc_list)
                st.download_button(f"⬇️ Download {bill_number} Vote Report", pdf, file_name=f"{bill_number.replace(' ','_')}_vote_report.pdf", mime="application/pdf")

        with col2:
            if st.button("Generate Bill Party Line Report PDF"):
                with st.spinner("Generating Bill Party Line PDF..."):
                    pdf = generate_bill_party_line_report(bill_number, bill_title, rc_list)
                st.download_button(f"⬇️ Download {bill_number} Party Line", pdf, file_name=f"{bill_number.replace(' ','_')}_party_line.pdf", mime="application/pdf")

        with col3:
            if st.button("Generate Both Bill Reports PDF"):
                with st.spinner("Generating Both Bill PDFs..."):
                    pdf_vote = generate_bill_vote_report(bill_number, bill_title, rc_list)
                    pdf_party = generate_bill_party_line_report(bill_number, bill_title, rc_list)
                st.download_button(f"⬇️ Download Vote Report", pdf_vote, file_name=f"{bill_number.replace(' ','_')}_vote_report.pdf", mime="application/pdf")
                st.download_button(f"⬇️ Download Party Line Report", pdf_party, file_name=f"{bill_number.replace(' ','_')}_party_line.pdf", mime="application/pdf")