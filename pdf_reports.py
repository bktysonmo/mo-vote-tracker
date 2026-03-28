from reportlab.lib.pagesizes import letter
from reportlab.lib import colors
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Table, TableStyle, Paragraph, Spacer
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.enums import TA_CENTER
from reportlab.pdfbase import pdfmetrics
from reportlab.pdfbase.ttfonts import TTFont
import io
import os
from datetime import datetime

# ------------------- FONT REGISTRATION -------------------
def _register_georgia():
    """Try to register Georgia TTF; fall back to Times-Roman if not found."""
    search_paths = [
        # Linux (msttcorefonts package)
        ("/usr/share/fonts/truetype/msttcorefonts/Georgia.ttf",
         "/usr/share/fonts/truetype/msttcorefonts/Georgiab.ttf"),
        # macOS system
        ("/Library/Fonts/Georgia.ttf",
         "/Library/Fonts/Georgia Bold.ttf"),
        # macOS user
        (os.path.expanduser("~/Library/Fonts/Georgia.ttf"),
         os.path.expanduser("~/Library/Fonts/Georgia Bold.ttf")),
        # Windows
        ("C:/Windows/Fonts/georgia.ttf",
         "C:/Windows/Fonts/georgiab.ttf"),
    ]
    for regular, bold in search_paths:
        if os.path.exists(regular):
            try:
                pdfmetrics.registerFont(TTFont("Georgia", regular))
                if os.path.exists(bold):
                    pdfmetrics.registerFont(TTFont("Georgia-Bold", bold))
                else:
                    pdfmetrics.registerFont(TTFont("Georgia-Bold", regular))
                return "Georgia", "Georgia-Bold"
            except Exception:
                pass
    # Graceful fallback — Times-Roman is a built-in PDF font, always available
    return "Times-Roman", "Times-Bold"

BODY_FONT, BOLD_FONT = _register_georgia()

# Page body width = 8.5" - 0.75" left - 0.75" right = 7.0"
PAGE_WIDTH = 7.0

# ------------------- STYLES -------------------
def get_styles():
    styles = getSampleStyleSheet()
    title_style = ParagraphStyle(
        "ReportTitle",
        parent=styles["Heading1"],
        fontName=BOLD_FONT,
        fontSize=16,
        alignment=TA_CENTER,
        spaceAfter=6
    )
    subtitle_style = ParagraphStyle(
        "ReportSubtitle",
        parent=styles["Normal"],
        fontName=BODY_FONT,
        fontSize=10,
        alignment=TA_CENTER,
        textColor=colors.grey,
        spaceAfter=4
    )
    section_style = ParagraphStyle(
        "SectionHeader",
        parent=styles["Heading2"],
        fontName=BOLD_FONT,
        fontSize=12,
        spaceBefore=12,
        spaceAfter=6
    )
    cell_style = ParagraphStyle(
        "CellText",
        parent=styles["Normal"],
        fontName=BODY_FONT,
        fontSize=8,
        leading=10,
        wordWrap="CJK"
    )
    small_cell_style = ParagraphStyle(
        "SmallCellText",
        parent=styles["Normal"],
        fontName=BODY_FONT,
        fontSize=7,
        leading=9,
        wordWrap="CJK"
    )
    normal_style = ParagraphStyle(
        "NormalGeorgia",
        parent=styles["Normal"],
        fontName=BODY_FONT,
        fontSize=10
    )
    return title_style, subtitle_style, section_style, normal_style, cell_style, small_cell_style

def wrap_cell(text, style):
    return Paragraph(str(text) if text else "", style)

# ------------------- TABLE HELPER -------------------
def make_table(data, col_widths=None, header_color=colors.HexColor("#2C3E50"), highlight_rows=None):
    """highlight_rows: dict {row_idx: color}"""
    if col_widths:
        widths = [w * inch for w in col_widths]
    else:
        widths = None

    table = Table(data, colWidths=widths, repeatRows=1)
    style = TableStyle([
        ("BACKGROUND", (0, 0), (-1, 0), header_color),
        ("TEXTCOLOR", (0, 0), (-1, 0), colors.white),
        ("FONTNAME", (0, 0), (-1, 0), BOLD_FONT),
        ("FONTSIZE", (0, 0), (-1, 0), 9),
        ("ALIGN", (0, 0), (-1, 0), "CENTER"),
        ("BOTTOMPADDING", (0, 0), (-1, 0), 8),
        ("TOPPADDING", (0, 0), (-1, 0), 8),
        ("FONTNAME", (0, 1), (-1, -1), BODY_FONT),
        ("FONTSIZE", (0, 1), (-1, -1), 8),
        ("VALIGN", (0, 0), (-1, -1), "MIDDLE"),
        ("TOPPADDING", (0, 1), (-1, -1), 4),
        ("BOTTOMPADDING", (0, 1), (-1, -1), 4),
        ("LEFTPADDING", (0, 0), (-1, -1), 6),
        ("RIGHTPADDING", (0, 0), (-1, -1), 6),
        ("GRID", (0, 0), (-1, -1), 0.5, colors.HexColor("#CCCCCC")),
    ])

    # Alternate row colors
    for i in range(1, len(data)):
        bg = colors.white if i % 2 else colors.HexColor("#F5F5F5")
        style.add("BACKGROUND", (0, i), (-1, i), bg)

    # Highlight specific rows if passed
    if highlight_rows:
        for row_idx, color in highlight_rows.items():
            style.add("BACKGROUND", (0, row_idx), (-1, row_idx), color)

    table.setStyle(style)
    return table

# ------------------- FOOTER -------------------
def report_footer(elements, normal_style):
    elements.append(Spacer(1, 0.3 * inch))
    timestamp = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    elements.append(Paragraph(
        f"Generated by Missouri Vote Tracker on {timestamp}. Data sourced from LegiScan LLC (CC BY 4.0).",
        ParagraphStyle(
            "Footer",
            parent=normal_style,
            fontName=BODY_FONT,
            fontSize=7,
            textColor=colors.grey,
            alignment=TA_CENTER
        )
    ))

# ------------------- LEGISLATOR VOTING RECORD -------------------
def generate_legislator_voting_record(name, party, chamber, district, summary_df, record_df):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter,
        leftMargin=0.75*inch, rightMargin=0.75*inch,
        topMargin=0.75*inch, bottomMargin=0.75*inch
    )
    title_style, subtitle_style, section_style, normal_style, cell_style, small_cell_style = get_styles()
    elements = []

    elements.append(Paragraph("Missouri Vote Tracker", subtitle_style))
    elements.append(Paragraph(f"Voting Record: {name}", title_style))
    elements.append(Paragraph(f"{party} | {chamber} | District {district}", subtitle_style))
    elements.append(Spacer(1, 0.2*inch))

    # Vote Summary
    # 2 cols: 3.0 + 1.5 = 4.5" (centered-ish, leaves breathing room)
    elements.append(Paragraph("Vote Summary", section_style))
    summary_data = [["Vote Type", "Count"]]
    for _, row in summary_df.iterrows():
        summary_data.append([row["vote_text"], str(row["count"])])
    elements.append(make_table(summary_data, col_widths=[3.0, 1.5]))
    elements.append(Spacer(1, 0.2*inch))

    # Full Voting Record
    # 6 cols summing to 7.0": Bill=0.7, Title=2.6, Date=0.8, Description=1.5, Result=0.75, Vote=0.65
    elements.append(Paragraph("Full Voting Record", section_style))
    record_data = [[
        wrap_cell("Bill", cell_style),
        wrap_cell("Title", cell_style),
        wrap_cell("Date", cell_style),
        wrap_cell("Description", cell_style),
        wrap_cell("Result", cell_style),
        wrap_cell("Vote Cast", cell_style),
    ]]
    highlight = {}
    for idx, row in enumerate(record_df.itertuples(), start=1):
        result = "Passed" if row.passed == 1 else "Failed"
        record_data.append([
            wrap_cell(row.bill_number, cell_style),
            wrap_cell(row.title, cell_style),
            wrap_cell(row.date, cell_style),
            wrap_cell(getattr(row, "description", ""), cell_style),
            wrap_cell(result, cell_style),
            wrap_cell(row.vote_text, cell_style),
        ])
        if getattr(row, "broke_rank", False):
            highlight[idx] = colors.HexColor("#FFF3CD")

    elements.append(make_table(
        record_data,
        col_widths=[0.70, 2.60, 0.80, 1.50, 0.75, 0.65],
        highlight_rows=highlight
    ))
    report_footer(elements, normal_style)
    doc.build(elements)
    buffer.seek(0)
    return buffer

# ------------------- LEGISLATOR PARTY LINE -------------------
def generate_legislator_party_line_report(name, party, chamber, district, party_line_df):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter,
        leftMargin=0.75*inch, rightMargin=0.75*inch,
        topMargin=0.75*inch, bottomMargin=0.75*inch
    )
    title_style, subtitle_style, section_style, normal_style, cell_style, small_cell_style = get_styles()
    elements = []

    elements.append(Paragraph("Missouri Vote Tracker", subtitle_style))
    elements.append(Paragraph(f"Party Line Report: {name}", title_style))
    elements.append(Paragraph(f"{party} | {chamber} | District {district}", subtitle_style))
    elements.append(Spacer(1, 0.2*inch))

    if not party_line_df.empty:
        # Party Unity Summary
        total_votes = len(party_line_df)
        with_party = len(party_line_df[party_line_df["broke_rank"] == False])
        unity_score = round((with_party / total_votes) * 100, 1) if total_votes > 0 else 0

        elements.append(Paragraph("Party Unity Summary", section_style))
        summary_data = [
            ["Metric", "Value"],
            ["Overall Party Unity Score", f"{unity_score}%"],
            ["Votes With Party", str(with_party)],
            ["Votes Breaking Rank", str(total_votes - with_party)],
            ["Total Votes Cast (Yea/Nay)", str(total_votes)]
        ]
        elements.append(make_table(summary_data, col_widths=[3.0, 1.5]))
        elements.append(Spacer(1, 0.2*inch))

        # Vote-by-vote party line detail
        # 8 cols summing to exactly 7.0":
        #   Bill=0.60, Title=1.80, Date=0.75, Description=1.30,
        #   Party Line=0.65, Member Vote=0.75, Broke Rank=0.65, Unity%=0.50
        elements.append(Paragraph("Vote-by-Vote Party Line Analysis", section_style))
        headers = ["Bill", "Title", "Date", "Description", "Party Line", "Member Vote", "Broke Rank", "Unity %"]
        detail_data = [[wrap_cell(h, cell_style) for h in headers]]
        highlight = {}
        for idx, row in enumerate(party_line_df.itertuples(), start=1):
            broke = "YES" if row.broke_rank else "No"
            detail_data.append([
                wrap_cell(row.bill_number, cell_style),
                wrap_cell(row.title, cell_style),
                wrap_cell(row.date, cell_style),
                wrap_cell(getattr(row, "description", ""), cell_style),
                wrap_cell(row.party_line, cell_style),
                wrap_cell(row.member_vote, cell_style),
                wrap_cell(broke, cell_style),
                wrap_cell(f"{row.unity_pct}%", cell_style),
            ])
            if row.broke_rank:
                highlight[idx] = colors.HexColor("#FFF3CD")

        elements.append(make_table(
            detail_data,
            col_widths=[0.60, 1.80, 0.75, 1.30, 0.65, 0.75, 0.65, 0.50],
            highlight_rows=highlight
        ))

    report_footer(elements, normal_style)
    doc.build(elements)
    buffer.seek(0)
    return buffer

# ------------------- BILL VOTE REPORT -------------------
def generate_bill_vote_report(bill_number, title, roll_calls):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter,
        leftMargin=0.75*inch, rightMargin=0.75*inch,
        topMargin=0.75*inch, bottomMargin=0.75*inch
    )
    title_style, subtitle_style, section_style, normal_style, cell_style, small_cell_style = get_styles()
    elements = []

    elements.append(Paragraph("Missouri Vote Tracker", subtitle_style))
    elements.append(Paragraph(f"Vote Report: {bill_number}", title_style))
    elements.append(Paragraph(title[:100], subtitle_style))
    elements.append(Spacer(1, 0.2*inch))

    for rc in roll_calls:
        result = "PASSED" if rc["passed"] == 1 else "FAILED"
        elements.append(Paragraph(f"{rc['date']} — {rc['description']} — {result}", section_style))

        # Totals: 3 equal cols = 7.0" / 3 ≈ 2.33 each
        totals_data = [
            ["Yea", "Nay", "NV/Absent"],
            [str(rc["yea"]), str(rc["nay"]), str(rc["nv"])]
        ]
        elements.append(make_table(totals_data, col_widths=[2.33, 2.33, 2.34]))
        elements.append(Spacer(1, 0.15*inch))

        # Individual votes
        # 5 cols summing to 7.0": Name=2.5, Party=0.6, Chamber=0.9, District=0.6, Vote=1.4
        vote_data = [[wrap_cell(h, small_cell_style) for h in ["Name", "Party", "Chamber", "District", "Vote"]]]
        for _, row in rc["detail_df"].iterrows():
            vote_data.append([
                wrap_cell(row["name"], small_cell_style),
                wrap_cell(row["party"], small_cell_style),
                wrap_cell(row["chamber"], small_cell_style),
                wrap_cell(str(row["district"]), small_cell_style),
                wrap_cell(row["vote_text"], small_cell_style),
            ])
        elements.append(make_table(vote_data, col_widths=[2.50, 0.60, 0.90, 0.60, 1.40]))
        elements.append(Spacer(1, 0.3*inch))

    report_footer(elements, normal_style)
    doc.build(elements)
    buffer.seek(0)
    return buffer

# ------------------- BILL PARTY LINE REPORT -------------------
def generate_bill_party_line_report(bill_number, title, roll_calls):
    buffer = io.BytesIO()
    doc = SimpleDocTemplate(
        buffer, pagesize=letter,
        leftMargin=0.75*inch, rightMargin=0.75*inch,
        topMargin=0.75*inch, bottomMargin=0.75*inch
    )
    title_style, subtitle_style, section_style, normal_style, cell_style, small_cell_style = get_styles()
    elements = []

    elements.append(Paragraph("Missouri Vote Tracker", subtitle_style))
    elements.append(Paragraph(f"Bill Party Line Report: {bill_number}", title_style))
    elements.append(Paragraph(title[:100], subtitle_style))
    elements.append(Spacer(1, 0.2*inch))

    for rc in roll_calls:
        result = "PASSED" if rc["passed"] == 1 else "FAILED"
        elements.append(Paragraph(f"{rc['date']} — {rc['description']} — {result}", section_style))

        # Party summary
        # 8 cols summing to 7.0":
        #   Party=0.55, Total=0.55, Yea=0.55, Nay=0.55, NV/Absent=0.65,
        #   Party Line=0.65, Unity%=0.55, Broke Rank=1.95
        if not rc["party_summary_df"].empty:
            summary_headers = ["Party", "Total", "Yea", "Nay", "NV/Absent", "Party Line", "Unity %", "Broke Rank"]
            summary_data = [[wrap_cell(h, cell_style) for h in summary_headers]]
            highlight = {}
            for idx, row in enumerate(rc["party_summary_df"].itertuples(), start=1):
                summary_data.append([
                    wrap_cell(row.Party, cell_style),
                    wrap_cell(row._2, cell_style),
                    wrap_cell(row.Yea, cell_style),
                    wrap_cell(row.Nay, cell_style),
                    wrap_cell(row._5, cell_style),
                    wrap_cell(row._6, cell_style),
                    wrap_cell(f"{row._7}%", cell_style),
                    wrap_cell(row._8, cell_style),
                ])
                if row._8 and row._8 != "None":
                    highlight[idx] = colors.HexColor("#FFF3CD")
            elements.append(make_table(
                summary_data,
                col_widths=[0.55, 0.55, 0.55, 0.55, 0.65, 0.65, 0.55, 1.95],
                highlight_rows=highlight
            ))
            elements.append(Spacer(1, 0.1*inch))

        # Individual votes
        # 5 cols summing to 7.0": Name=2.5, Party=0.6, Chamber=0.9, District=0.6, Vote=1.4
        if not rc["detail_df"].empty:
            vote_headers = ["Name", "Party", "Chamber", "District", "Vote"]
            vote_data = [[wrap_cell(h, small_cell_style) for h in vote_headers]]
            highlight = {}
            for idx, row in enumerate(rc["detail_df"].itertuples(), start=1):
                vote_data.append([
                    wrap_cell(row.name, small_cell_style),
                    wrap_cell(row.party, small_cell_style),
                    wrap_cell(row.chamber, small_cell_style),
                    wrap_cell(str(row.district), small_cell_style),
                    wrap_cell(row.vote_text, small_cell_style),
                ])
                if getattr(row, "broke_rank", False):
                    highlight[idx] = colors.HexColor("#FFF3CD")
            elements.append(make_table(
                vote_data,
                col_widths=[2.50, 0.60, 0.90, 0.60, 1.40],
                highlight_rows=highlight
            ))
            elements.append(Spacer(1, 0.2*inch))

    report_footer(elements, normal_style)
    doc.build(elements)
    buffer.seek(0)
    return buffer