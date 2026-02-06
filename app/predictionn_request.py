import joblib
import pandas as pd

from db import get_connection
from collect_data import explain_query  # on réutilise ta fonction déjà correcte

MODEL_PATH = "sadop_xgb_model.joblib"


def build_features_for_sql(sql: str) -> pd.DataFrame:
    """
    Construit UNE ligne de features à partir d'une requête SQL brute.
    On utilise EXPLAIN pour récupérer type/key/rows/extra.
    Les autres features numériques sont estimées de façon simple.
    """

    # 1) Récupérer le plan d'exécution
    info = explain_query(sql)  # retourne explain_type, explain_key, explain_rows, explain_extra

    # 2) Features numériques (approximation raisonnable pour un test "online")
    exec_count = 1                 # on suppose 1 exécution
    total_time_s = 0.0             # inconnu -> 0
    avg_time_s = 0.0               # inconnu -> 0
    rows_examined = info.get("explain_rows") or 0
    rows_sent = 0                  # on ne calcule pas vraiment ici
    explain_rows = info.get("explain_rows") or 0

    row = {
        # colonnes numériques
        "exec_count": exec_count,
        "total_time_s": total_time_s,
        "avg_time_s": avg_time_s,
        "rows_examined": rows_examined,
        "rows_sent": rows_sent,
        "explain_rows": explain_rows,

        # colonnes catégorielles
        "explain_type": info.get("explain_type", "UNKNOWN"),
        "explain_key": info.get("explain_key", "UNKNOWN"),
        "explain_extra": info.get("explain_extra", "UNKNOWN"),
    }

    # On retourne un DataFrame avec une seule ligne
    return pd.DataFrame([row])


def predict_sql(sql: str):
    """
    Charge le modèle, construit les features pour la requête SQL,
    puis affiche la prédiction + probas.
    """
    print("\n================ PREDICTION POUR ==================")
    print(sql)
    print("===================================================\n")

    # Charger le modèle (pipeline: preprocessor + XGBoost)
    clf = joblib.load(MODEL_PATH)

    # Construire les features pour cette requête
    X = build_features_for_sql(sql)

    # Prédiction
    proba = clf.predict_proba(X)[0]
    label = clf.predict(X)[0]

    label_str = "SLOW QUERY" if label == 1 else "FAST QUERY"

    print("=== Prediction result ===")
    print(f"Predicted label: {label}  -> {label_str}")
    print(f"Prediction probabilities: [P(fast)= {proba[0]:.4f} , P(slow)= {proba[1]:.4f}]")
    print("\n---------------------------------------------------")


if  __name__ == "__main__":
    # 🔥 ICI tu mets tes requêtes “difficiles”
    test_queries = [
        # 1) JOIN + GROUP BY
        """
        SELECT u.city, AVG(s.duration) AS avg_duration
        FROM users_small u
        JOIN sessions_small s ON u.id = s.user_id
        GROUP BY u.city
        ORDER BY avg_duration DESC
        """,

        # 2) JOIN + GROUP BY + COUNT
        """
        SELECT u.city,
               AVG(s.duration) AS avg_duration,
               COUNT(*) AS nb_sessions
        FROM users_small u
        JOIN sessions_small s ON u.id = s.user_id
        GROUP BY u.city
        ORDER BY avg_duration DESC
        """,

        # 3) Sous-requête corrélée
        """
        SELECT u.id, u.city
        FROM users_small u
        WHERE (
          SELECT AVG(s.duration)
          FROM sessions_small s
          WHERE s.user_id = u.id
        ) > 70
        """,

        # 4) EXISTS
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

        # 5) Double agrégation
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
        """,

        # 6) IN avec sous-requête
        """
        SELECT *
        FROM users_small
        WHERE id IN (
          SELECT user_id FROM sessions_small
          WHERE duration > 50
        )
        """,
    ]

    for q in test_queries:
        predict_sql(q.strip())
