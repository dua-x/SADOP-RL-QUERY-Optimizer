import joblib
from db import get_connection
from explain_utils import explain_query   # ta fonction existante
from metrics import execute_query_once, get_real_metrics

MODEL_PATH = "sadop_xgb_model.joblib"

def predict_real(sql: str):
    print("\n================ PREDICTION RÉELLE =================")
    print(sql)
    print("====================================================")

    # 1️⃣ Exécuter réellement la requête
    execute_query_once(sql)

    # 2️⃣ Lire les vraies métriques
    metrics = get_real_metrics(sql)

    # 3️⃣ EXPLAIN
    explain = explain_query(sql)

    # 4️⃣ Construire l'entrée du modèle
    sample = {
        "exec_count": metrics["exec_count"],
        "total_time_s": metrics["avg_time_s"] * metrics["exec_count"],
        "avg_time_s": metrics["avg_time_s"],
        "rows_examined": metrics["rows_examined"],
        "rows_sent": metrics["rows_sent"],
        "explain_type": explain["explain_type"],
        "explain_key": explain["explain_key"],
        "explain_rows": explain["explain_rows"] or 0,
        "explain_extra": explain["explain_extra"],
    }

    model = joblib.load(MODEL_PATH)

    import pandas as pd
    X = pd.DataFrame([sample])

    proba = model.predict_proba(X)[0]
    pred = model.predict(X)[0]

    print("\n=== Résultat ===")
    print("Label prédit :", "SLOW" if pred == 1 else "FAST")
    print(f"P(fast)={proba[0]:.4f} | P(slow)={proba[1]:.4f}")

    print("\n=== Métriques réelles ===")
    for k, v in metrics.items():
        print(f"{k}: {v}")
