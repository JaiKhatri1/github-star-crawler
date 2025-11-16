import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv
from urllib.parse import urlparse

# Load environment variables
load_dotenv()
DATABASE_URL = os.getenv("DATABASE_URL")

# Parse DATABASE_URL
result = urlparse(DATABASE_URL)
conn = psycopg2.connect(
    dbname=result.path[1:],  # remove leading /
    user=result.username,
    password=result.password,
    host=result.hostname,
    port=result.port
)

cur = conn.cursor()

# 1️⃣ Top starred repos
cur.execute("""
    SELECT name_with_owner, stars 
    FROM repos 
    ORDER BY stars DESC 
    LIMIT 10;
""")
top_repos = cur.fetchall()
print("Top 10 starred repos:")
for r in top_repos:
    print(r)

# 2️⃣ Count total repos crawled
cur.execute("SELECT COUNT(*) FROM repos;")
total = cur.fetchone()[0]
print(f"\nTotal repos crawled: {total}")

# 3️⃣ Export to CSV
cur.execute("SELECT * FROM repos;")
rows = cur.fetchall()
cols = [desc[0] for desc in cur.description]

# Use pandas to easily write CSV
df = pd.DataFrame(rows, columns=cols)
import os
csv_path = os.path.join(os.getcwd(), "repos.csv")
df.to_csv(csv_path, index=False)
print(f"CSV saved to {csv_path}")

cur.close()
conn.close()
