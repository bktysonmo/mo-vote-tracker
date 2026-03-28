from database import get_connection

conn = get_connection()
c = conn.cursor()

print("Sample legislators:")
c.execute('SELECT people_id, name FROM legislators LIMIT 5')
for row in c.fetchall():
    print(row["people_id"], row["name"])

print("\nSample member_vote people_ids:")
c.execute('SELECT DISTINCT people_id FROM member_votes LIMIT 5')
for row in c.fetchall():
    print(row["people_id"])

conn.close()