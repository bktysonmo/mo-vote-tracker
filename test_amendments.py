from amendment_fetcher import fetch_house_amendments, fetch_senate_amendments, fetch_amendment_text, get_senate_bill_map

print("Testing House - HB1625:")
house = fetch_house_amendments("HB1625")
for a in house:
    print(f"  {a['amendment_code']} | {a['status']} | Sponsor: {a['sponsor']} | Floor: {a['floor_number']}")
    if a['pdf_url']:
        print(f"  Fetching PDF text...")
        text = fetch_amendment_text(a['pdf_url'])
        print(f"  Text preview: {text[:200] if text else 'None'}")

print("\nTesting Senate - SB 835:")
senate_map = get_senate_bill_map()
senate = fetch_senate_amendments("SB 835", senate_map.get("SB 835", ""))
for a in senate:
    print(f"  {a['amendment_code']} | {a['status']} | Name: {a['amendment_name']}")
    if a['pdf_url']:
        print(f"  Fetching PDF text...")
        text = fetch_amendment_text(a['pdf_url'])
        print(f"  Text preview: {text[:200] if text else 'None'}")