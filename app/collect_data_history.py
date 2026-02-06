import pandas as pd
import mysql.connector
from db import get_connection


def explain_query(sql: str):
    """
    EXPLAIN sur la requête SQL réelle (SQL_TEXT).
    Si EXPLAIN échoue, on renvoie des valeurs 'UNKNOWN'.
    """

    sql_stripped = sql.strip()

    # On ne fait EXPLAIN que sur des SELECT
    if not sql_stripped.upper().startswith("SELECT"):
        return {
            "explain_type": "UNKNOWN",
            "explain_key": "UNKNOWN",
            "explain_rows": None,
            "explain_extra": "NON_SELECT",
        }

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    try:
        explain_sql = f"EXPLAIN {sql_stripped}"
        cursor.execute(explain_sql)

        rows = cursor.fetchall()
        if not rows:
            return {
                "explain_type": "UNKNOWN",
                "explain_key": "UNKNOWN",
                "explain_rows": None,
                "explain_extra": None,
            }

        row = rows[0]

        return {
            "explain_type": row.get("type", "UNKNOWN"),
            "explain_key": row.get("key", "UNKNOWN"),
            "explain_rows": row.get("rows", None),
            "explain_extra": row.get("Extra", None),
        }

    except mysql.connector.Error as e:
        print(f"[WARN] EXPLAIN failed for: {sql_stripped[:80]} ...  -> {e}")
        return {
            "explain_type": "UNKNOWN",
            "explain_key": "UNKNOWN",
            "explain_rows": None,
            "explain_extra": "EXPLAIN_ERROR",
        }
    finally:
        cursor.close()
        conn.close()


def collect_dataset_from_history(limit: int = 2000, label_threshold: float = 0.05) -> pd.DataFrame:
    """
    Lit les requêtes dans performance_schema.events_statements_history_long.
    Chaque exécution est un échantillon.
    """

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    sql = """
    SELECT
      DIGEST_TEXT,
      SQL_TEXT,
      TIMER_WAIT / 1e12 AS time_s,
      ROWS_EXAMINED,
      ROWS_SENT
    FROM performance_schema.events_statements_history_long
    WHERE DIGEST_TEXT IS NOT NULL
      AND SQL_TEXT IS NOT NULL
      AND SQL_TEXT NOT LIKE 'SELECT DIGEST_TEXT,%events_statements_summary_by_digest%'
    ORDER BY TIMER_WAIT DESC
    LIMIT %s
    """

    cursor.execute(sql, (limit,))
    rows = cursor.fetchall()
    print(f"[INFO] Fetched {len(rows)} statement executions from history_long.")

    records = []

    for r in rows:
        digest_text = r["DIGEST_TEXT"]
        sql_text = r["SQL_TEXT"]
        time_s = r["time_s"] or 0.0
        rows_examined = r["ROWS_EXAMINED"] or 0
        rows_sent = r["ROWS_SENT"] or 0

        # Label lente/rapide basé sur le temps réel
        label_slow = 1 if time_s > label_threshold else 0

        explain_info = explain_query(sql_text)

        record = {
            "DIGEST_TEXT": digest_text,
            "SQL_TEXT": sql_text,
            "exec_count": 1,
            "total_time_s": time_s,
            "avg_time_s": time_s,
            "rows_examined": rows_examined,
            "rows_sent": rows_sent,
            "explain_type": explain_info["explain_type"],
            "explain_key": explain_info["explain_key"],
            "explain_rows": explain_info["explain_rows"],
            "explain_extra": explain_info["explain_extra"],
            "label_slow": label_slow,
        }

        records.append(record)

    cursor.close()
    conn.close()

    df = pd.DataFrame(records)
    print("[INFO] label_slow distribution:")
    print(df["label_slow"].value_counts())
    return df


if __name__ == "__main__":
    df = collect_dataset_from_history(limit=2000, label_threshold=0.05)
    df.to_csv("raw_dataset.csv", index=False)
    print("Saved raw_dataset.csv from history_long")
