import pandas as pd
import mysql.connector
from mysql.connector import errors
from db import get_connection


def explain_query(sql: str):
    """
    EXPLAIN la requête si c'est un SELECT.
    Retourne des infos simplifiées; en cas d'erreur, renvoie UNKNOWN.
    """
    sql_stripped = sql.strip()

    if not sql_stripped.upper().startswith("SELECT"):
        return {
            "explain_type": "NON_SELECT",
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
        print(f"[WARN] EXPLAIN failed for: {sql_stripped[:80]} ... -> {e}")
        return {
            "explain_type": "UNKNOWN",
            "explain_key": "UNKNOWN",
            "explain_rows": None,
            "explain_extra": "EXPLAIN_ERROR",
        }
    finally:
        cursor.close()
        conn.close()


def collect_dataset(limit: int = 5000, slow_fraction: float = 0.3) -> pd.DataFrame:
    """
    - Récupère jusqu'à limit requêtes agrégées depuis performance_schema.
    - Calcule label_slow en se basant sur les 30% requêtes les plus lentes
      selon avg_time_s ET rows_examined.
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
    cursor.close()
    conn.close()

    print(f"[INFO] Fetched {len(rows)} statements from performance_schema.")

    if not rows:
        print("[WARN] No rows fetched. Did you run workload_simulator.py ?")
        return pd.DataFrame()

    df = pd.DataFrame(rows)

    # Nettoyage de base
    df["avg_time_s"] = df["avg_time_s"].fillna(0.0)
    df["rows_examined"] = df["rows_examined"].fillna(0)

    n_before = len(df)
    print(f"[INFO] Dataset size before filtering: {n_before}")

    # On enlève seulement les requêtes ultra-bizarres (aucun temps et aucun row examiné)
    df = df[(df["avg_time_s"] > 0) | (df["rows_examined"] > 0)].copy()
    print(f"[INFO] Dataset size after basic filter: {len(df)}")

    if len(df) == 0:
        print("[WARN] After filter, dataset is empty.")
        return df

    # --------- Définition des requêtes lentes (top 30%) ----------
    # seuil sur avg_time_s (70ème percentile)
    q_time = df["avg_time_s"].quantile(0.7)
    # seuil sur rows_examined (70ème percentile)
    q_rows = df["rows_examined"].quantile(0.7)

    print(f"[INFO] Quantiles used for slow label:")
    print(f"       avg_time_s 70% = {q_time:.6f} s")
    print(f"       rows_examined 70% = {q_rows}")

    # Lente si temps élevé OU beaucoup de lignes examinées
    df["label_slow"] = (
        (df["avg_time_s"] >= q_time) |
        (df["rows_examined"] >= q_rows)
    ).astype(int)

    print("[INFO] Label distribution:\n", df["label_slow"].value_counts())

    df.to_csv("raw_dataset.csv", index=False)
    print("Saved raw_dataset.csv")

    return df


if __name__ == "__main__":
    # On vise ~30% de requêtes lentes
    collect_dataset(limit=15000, slow_fraction=0.3)