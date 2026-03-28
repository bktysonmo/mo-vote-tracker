import sqlite3

conn = sqlite3.connect("mo_history.db")
c = conn.cursor()

c.execute("SELECT COUNT(*) FROM bills")
print("Bills:", c.fetchone()[0])

c.execute("SELECT COUNT(*) FROM votes")
print("Votes:", c.fetchone()[0])

c.execute("SELECT COUNT(*) FROM member_votes")
print("Member votes:", c.fetchone()[0])

c.execute("SELECT COUNT(*) FROM legislators")
print("Legislator records:", c.fetchone()[0])

c.execute("""
    SELECT COUNT(*) FROM member_votes mv
    JOIN votes v ON mv.roll_call_id = v.roll_call_id
""")
print("Member votes with matching roll calls:", c.fetchone()[0])

print("\nVotes by session:")
c.execute("""
    SELECT s.session_name, COUNT(v.roll_call_id) as vote_count,
           COUNT(mv.id) as member_vote_count
    FROM sessions s
    LEFT JOIN votes v ON s.session_id = v.session_id
    LEFT JOIN member_votes mv ON v.roll_call_id = mv.roll_call_id
    GROUP BY s.session_name
    ORDER BY s.year DESC, s.special ASC
""")
for row in c.fetchall():
    print(f"  {row[0]}: {row[1]} roll calls, {row[2]} member votes")

conn.close()