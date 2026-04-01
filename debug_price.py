import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
from rapidfuzz import fuzz, process
from bot.data.db import DB_PATH

conn = sqlite3.connect(DB_PATH)
cur = conn.cursor()
cur.execute("SELECT DISTINCT loading_locality FROM applications_archive WHERE loading_locality IS NOT NULL")
localities = [r[0] for r in cur.fetchall()]
conn.close()

target = "Киевское"
print("=== partial_ratio ===")
matches = process.extract(target, localities, scorer=fuzz.partial_ratio, limit=5)
for m in matches:
    print(f"  {m[1]:.0f}% - {m[0]}")
