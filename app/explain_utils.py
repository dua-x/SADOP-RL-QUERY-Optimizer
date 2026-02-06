import mysql.connector
from db import get_connection


def explain_query(sql: str):
    """
    Exécute EXPLAIN sur une requête SELECT et retourne les vraies infos du plan.
    """

    sql = sql.strip()

    if not sql.upper().startswith("SELECT"):
        return {
            "explain_type": "NON_SELECT",
            "explain_key": "NONE",
            "explain_rows": 0,
            "explain_extra": "NON_SELECT",
        }

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        cursor.execute(f"EXPLAIN {sql}")
        rows = cursor.fetchall()

        if not rows:
            return {
                "explain_type": "UNKNOWN",
                "explain_key": "UNKNOWN",
                "explain_rows": 0,
                "explain_extra": "EMPTY",
            }

        r = rows[0]
        return {
            "explain_type": r.get("type", "UNKNOWN"),
            "explain_key": r.get("key", "UNKNOWN"),
            "explain_rows": r.get("rows", 0),
            "explain_extra": r.get("Extra", ""),
        }

    except mysql.connector.Error as e:
        print(f"[WARN] EXPLAIN failed: {e}")
        return {
            "explain_type": "ERROR",
            "explain_key": "ERROR",
            "explain_rows": 0,
            "explain_extra": "ERROR",
        }

    finally:
        cursor.close()
        conn.close()
