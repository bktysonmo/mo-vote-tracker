from summary_fetcher import setup_similar_bills_table, fetch_house_summary, extract_similar_bills

setup_similar_bills_table()

text, version = fetch_house_summary("HB1607")
print("Version found:", version)
print("Text snippet:", text[:300] if text else "None")

if text:
    refs = extract_similar_bills(text, "HB1607")
    print("Similar refs:", refs)