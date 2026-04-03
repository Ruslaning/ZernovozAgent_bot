"""Load distances reference from Google Sheets into SQLite."""
import sys, sqlite3
sys.stdout.reconfigure(encoding='utf-8')
import gspread
from google.oauth2.service_account import Credentials

SERVICE_ACCOUNT = r'C:\Users\Kseni\.openclaw\workspace-reader\service_account.json'
SHEET_ID = '1YbQDb9fQ8K--D9UAF638UBAsGvkfF3oq7rAfiupsCA8'
DB_PATH = r'C:\Users\Kseni\zernovoz-agent\zernovoz.db'

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']
creds = Credentials.from_service_account_file(SERVICE_ACCOUNT, scopes=SCOPES)
gc = gspread.authorize(creds)

print("Читаю лист 'справочник расстояний'...")
sh = gc.open_by_key(SHEET_ID)
ws = sh.worksheet('справочник расстояний')
all_rows = ws.get_all_values()

# Headers (row 0): A=адрес откуда, B=район, C=Регион, D=empty, E=full_address
#                  F=Ново, G=Волна, H=Азов, I=разница, J=ККЗ, K=Северская, L=Гулькевичи, M=Гиагинская

def safe_int(val):
    try:
        v = str(val).strip().replace(' ', '').replace('\xa0', '')
        return int(v) if v else None
    except:
        return None

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("DELETE FROM distances")

inserted = 0
for i, row in enumerate(all_rows[1:], start=2):
    if not row or not row[0].strip():
        continue

    address    = row[0].strip() if len(row) > 0 else ''
    district   = row[1].strip() if len(row) > 1 else ''
    region     = row[2].strip() if len(row) > 2 else ''
    full_addr  = row[4].strip() if len(row) > 4 else ''

    if not full_addr:
        full_addr = ', '.join(filter(None, [region, district, address]))

    dist_novo  = safe_int(row[5]) if len(row) > 5 else None   # F = Новороссийск
    dist_taman = safe_int(row[6]) if len(row) > 6 else None   # G = Волна/Тамань
    dist_azov  = safe_int(row[7]) if len(row) > 7 else None   # H = Азов
    dist_kkz   = safe_int(row[9]) if len(row) > 9 else None   # J = ККЗ
    dist_sev   = safe_int(row[10]) if len(row) > 10 else None # K = Северская
    dist_gulk  = safe_int(row[11]) if len(row) > 11 else None # L = Гулькевичи
    dist_giag  = safe_int(row[12]) if len(row) > 12 else None # M = Гиагинская

    cur.execute("""
        INSERT OR REPLACE INTO distances
        (full_address, address, district, region,
         dist_novorossiysk, dist_taman, dist_azov, dist_kkz,
         dist_severskaya, dist_gulkevichi, dist_giaginskaya)
        VALUES (?,?,?,?,?,?,?,?,?,?,?)
    """, (full_addr, address, district, region,
          dist_novo, dist_taman, dist_azov, dist_kkz,
          dist_sev, dist_gulk, dist_giag))
    inserted += 1

conn.commit()
conn.close()
print(f"Загружено {inserted} записей")

# Verify
conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
cur = conn.cursor()
cur.execute("SELECT address, full_address, dist_novorossiysk FROM distances WHERE address LIKE '%льв%'")
rows = cur.fetchall()
print(f"\nПроверка - Льв*: {len(rows)} записей")
for r in rows:
    print(f"  {r['address']} | {r['full_address']} | ново={r['dist_novorossiysk']}")
conn.close()
