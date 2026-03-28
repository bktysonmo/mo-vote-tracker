import requests, os, io, json, zipfile, base64
from dotenv import load_dotenv
load_dotenv()

API_KEY = os.getenv("LEGISCAN_API_KEY")

# Download just the 2026 session dataset and inspect one bill's structure
r = requests.get("https://api.legiscan.com/", params={
    "key": API_KEY,
    "op": "getDataset",
    "id": 2239,
    "access_key": "6UzBHz4T6KinkHtOIQfrUK"
})

data = r.json()
zip_b64 = data.get("dataset", {}).get("zip", "")
zip_bytes = base64.b64decode(zip_b64)
zip_buffer = io.BytesIO(zip_bytes)

with zipfile.ZipFile(zip_buffer, "r") as zf:
    all_files = zf.namelist()
    print("Files in ZIP (first 20):")
    for f in all_files[:20]:
        print(" ", f)

    # Find a bill file and a vote file
    bill_files = [f for f in all_files if "/bill/" in f and f.endswith(".json")]
    vote_files = [f for f in all_files if "/vote/" in f and f.endswith(".json")]
    people_files = [f for f in all_files if "/people/" in f and f.endswith(".json")]

    print(f"\nBill files: {len(bill_files)}")
    print(f"Vote files: {len(vote_files)}")
    print(f"People files: {len(people_files)}")

    # Look at one bill file structure
    if bill_files:
        with zf.open(bill_files[0]) as f:
            bill = json.load(f)
        print("\nBill file top-level keys:", list(bill.keys()))
        inner = bill.get("bill", bill)
        print("Inner bill keys:", list(inner.keys()))
        print("votes field:", inner.get("votes", "NOT FOUND"))

    # Look at one vote file if they exist
    if vote_files:
        with zf.open(vote_files[0]) as f:
            vote = json.load(f)
        print("\nVote file top-level keys:", list(vote.keys()))
        inner_vote = vote.get("roll_call", vote)
        print("Inner vote keys:", list(inner_vote.keys()))
        print("Sample votes list (first 3):", inner_vote.get("votes", [])[:3])