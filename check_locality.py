import sqlite3, sys
sys.stdout.reconfigure(encoding='utf-8')
conn = sqlite3.connect(r'C:\Users\Kseni\zernovoz-agent\zernovoz.db')
cur = conn.cursor()

cur.execute("""SELECT unloading_terminal, distance, price_per_kg, culture
    FROM applications_archive
    WHERE loading_locality = 'Киевка село'
    AND date(fetched_at) >= date('now', '-5 days')""")
print('Киевка село за 5 дней:', cur.fetchall())

cur.execute("""SELECT unloading_terminal, distance, price_per_kg, culture
    FROM applications_archive
    WHERE loading_locality = 'село Киевское, Крымский район'
    AND date(fetched_at) >= date('now', '-5 days')""")
print('село Киевское за 5 дней:', cur.fetchall())
conn.close()
