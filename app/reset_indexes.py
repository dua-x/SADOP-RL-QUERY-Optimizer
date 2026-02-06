# reset_indexes.py
import mysql.connector
from db import get_connection

conn = get_connection()
cursor = conn.cursor()

cursor.execute("""
SELECT table_name, index_name
FROM information_schema.statistics
WHERE table_schema = 'sadop_db'
  AND index_name <> 'PRIMARY'
""")

indexes = cursor.fetchall()

for table, index in indexes:
    sql = f"ALTER TABLE {table} DROP INDEX `{index}`"
    print("[DROP]", sql)
    try:
        cursor.execute(sql)
    except Exception as e:
        print("[WARN]", e)

conn.commit()
cursor.close()
conn.close()

print("✅ Tous les indexes secondaires ont été supprimés.")