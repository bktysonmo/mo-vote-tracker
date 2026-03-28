from database import get_connection

conn = get_connection()
c = conn.cursor()

c.execute('SELECT COUNT(*) FROM legislators')
print('Legislators:', c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM bills')
print('Bills:', c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM votes')
print('Votes:', c.fetchone()[0])

c.execute('SELECT COUNT(*) FROM member_votes')
print('Member votes:', c.fetchone()[0])

conn.close()