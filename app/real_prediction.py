import time
import joblib
import pandas as pd
from db import get_connection
from explain_utils import explain_query


MODEL_PATH = "sadop_xgb_model.joblib"


def run_real_prediction(sql: str):
    print("\n================ PREDICTION POUR ==================")
    print(sql)
    print("===================================================")

    conn = get_connection()
    cursor = conn.cursor(dictionary=True)

    # --- 1. EXPLAIN ---
    explain_info = explain_query(sql)

    # --- 2. Exécution réelle (mesure du temps) ---
    start = time.time()
    cursor.execute(sql)
    rows = cursor.fetchall()
    end = time.time()

    exec_time = end - start
    rows_sent = len(rows)

    # --- 3. Récupérer rows_examined ---
    cursor.execute("SHOW SESSION STATUS LIKE 'Handler_read%'")
    handler_stats = cursor.fetchall()
    rows_examined = sum(int(r["Value"]) for r in handler_stats if r["Value"].isdigit())

    conn.close()

    # --- 4. Construire l'entrée ML ---
    X = pd.DataFrame([{
        "exec_count": 1,
        "total_time_s": exec_time,
        "avg_time_s": exec_time,
        "rows_examined": rows_examined,
        "rows_sent": rows_sent,
        "explain_rows": explain_info["explain_rows"],
        "explain_type": explain_info["explain_type"],
        "explain_key": explain_info["explain_key"],
        "explain_extra": explain_info["explain_extra"],
    }])

    # --- 5. Charger modèle ---
    clf = joblib.load(MODEL_PATH)

    proba = clf.predict_proba(X)[0]
    pred = clf.predict(X)[0]

    print("\n=== MÉTRIQUES RÉELLES ===")
    print(f"avg_time_s     : {exec_time:.6f}")
    print(f"rows_examined  : {rows_examined}")
    print(f"rows_sent      : {rows_sent}")

    print("\n=== PRÉDICTION ML ===")
    print("Label prédit :", "SLOW QUERY" if pred == 1 else "FAST QUERY")
    print(f"P(fast)= {proba[0]:.4f} | P(slow)= {proba[1]:.4f}")
    print("---------------------------------------------------")


if __name__ == "__main__":
    TEST_QUERIES = [

        # Requête simple
        "SELECT * FROM users_small WHERE city = 'Paris' LIMIT 100",

        # JOIN + GROUP BY
        """
        SELECT u.city, AVG(s.duration) AS avg_duration
        FROM users_small u
        JOIN sessions_small s ON u.id = s.user_id
        GROUP BY u.city
        ORDER BY avg_duration DESC
        """,

        # Sous-requête corrélée (lente)
        """
        SELECT u.id, u.city
        FROM users_small u
        WHERE (
            SELECT AVG(s.duration)
            FROM sessions_small s
            WHERE s.user_id = u.id
        ) > 70
        """,

        # EXISTS
        """
        SELECT u.id, u.city
        FROM users_small u
        WHERE EXISTS (
            SELECT 1
            FROM sessions_small s
            WHERE s.user_id = u.id
            AND s.calories > 600
        )
        """,

        # Très lourde (double agrégation)
        """
        SELECT city, AVG(user_avg) AS avg_city_duration
        FROM (
            SELECT u.id, u.city, AVG(s.duration) AS user_avg
            FROM users_small u
            JOIN sessions_small s ON u.id = s.user_id
            GROUP BY u.id, u.city
        ) t
        GROUP BY city
        ORDER BY avg_city_duration DESC
        """
    ]

    for q in TEST_QUERIES:
        run_real_prediction(q)
