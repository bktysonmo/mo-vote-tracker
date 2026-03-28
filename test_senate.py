from summary_fetcher import build_senate_bill_map, fetch_senate_summary, extract_similar_bills

senate_map = build_senate_bill_map()
print("Sample from map:", list(senate_map.items())[:5])

text, version = fetch_senate_summary("SB 1558", senate_map)
print("Version found:", version)
print("Text snippet:", text[:300] if text else "None")

if text:
    refs = extract_similar_bills(text, "SB 1558")
    print("Similar refs:", refs)