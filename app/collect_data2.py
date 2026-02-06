import pandas as pd
import mysql.connector
from db import get_connection


def explain_query(sql: str):
    """
    Tente d'exécuter un EXPLAIN sur la requête SQL donnée.
    Retourne un dict avec type, key, rows, extra.
    Si EXPLAIN échoue, retourne des valeurs 'UNKNOWN'.
    IMPORTANT : on consomme TOUTES les lignes (fetchall) pour éviter l'erreur "Unread result found".
    """

    sql_stripped = sql.strip()

    # On ne fait EXPLAIN que sur des SELECT pour éviter des erreurs inutiles
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

        # On lit TOUTES les lignes pour ne rien laisser en attente
        rows = cursor.fetchall()
        if not rows:
            return {
                "explain_type": "UNKNOWN",
                "explain_key": "UNKNOWN",
                "explain_rows": None,
                "explain_extra": None,
            }

        # On prend seulement la première ligne du plan (suffisant pour notre cas)
        row = rows[0]

        return {
            "explain_type": row.get("type", "UNKNOWN"),
            "explain_key": row.get("key", "UNKNOWN"),
            "explain_rows": row.get("rows", None),
            "explain_extra": row.get("Extra", None),
        }

    except mysql.connector.Error as e:
        print(f"[WARN] EXPLAIN failed for: {sql_stripped[:60]} ...  -> {e}")
        return {
            "explain_type": "UNKNOWN",
            "explain_key": "UNKNOWN",
            "explain_rows": None,
            "explain_extra": "EXPLAIN_ERROR",
        }
    finally:
        cursor.close()
        conn.close()


def collect_dataset(limit: int = 200, label_threshold: float = 0.05) -> pd.DataFrame:
    """
    Récupère les requêtes depuis performance_schema,
    calcule des métriques (avg_time_s, rows_examined, etc.),
    applique un EXPLAIN et ajoute un label 'label_slow'.
    """
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    sql = """
    SELECT
      DIGEST_TEXT,
      COUNT_STAR AS exec_count,
      SUM_TIMER_WAIT / 1e12 AS total_time_s,
      IFNULL(SUM_TIMER_WAIT/NULLIF(COUNT_STAR,0),0) / 1e12 AS avg_time_s,
      SUM_ROWS_EXAMINED AS rows_examined,
      SUM_ROWS_SENT AS rows_sent
    FROM performance_schema.events_statements_summary_by_digest
    WHERE DIGEST_TEXT IS NOT NULL
    ORDER BY SUM_TIMER_WAIT DESC
    LIMIT %s
    """

    cursor.execute(sql, (limit,))
    rows = cursor.fetchall()
    print(f"Fetched {len(rows)} statements.")

    records = []

    for r in rows:
        digest_text = r["DIGEST_TEXT"]

        # EXPLAIN (ou valeurs UNKNOWN si pas applicable)
        explain_info = explain_query(digest_text)

        avg_time_s = r["avg_time_s"] or 0.0
        label_slow = 1 if avg_time_s > label_threshold else 0

        record = {
            "DIGEST_TEXT": digest_text,
            "exec_count": r["exec_count"],
            "total_time_s": r["total_time_s"],
            "avg_time_s": avg_time_s,
            "rows_examined": r["rows_examined"],
            "rows_sent": r["rows_sent"],
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
    print(df["label_slow"].value_counts())
    return df


if __name__ == "__main__":
    # On collecte plus de requêtes, avec un seuil de lenteur légèrement plus haut
    df = collect_dataset(limit=500, label_threshold=0.05)
    df.to_csv("raw_dataset.csv", index=False)
    print("Saved raw_dataset.csv")
