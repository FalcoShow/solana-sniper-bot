import os, psycopg2, urllib.parse as up
url=os.environ.get("DATABASE_URL")
print("DATABASE_URL:", url)
conn = psycopg2.connect(url)
cur = conn.cursor()
cur.execute("select version();")
print("Postgres version:", cur.fetchone()[0])
cur.close(); conn.close()
print("PG OK ✅")
