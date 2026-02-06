import pandas as pd
import joblib
import numpy as np


MODEL_PATH = "sadop_xgb_model.joblib"
DATA_PATH = "raw_dataset.csv"


def main():
    print("[INFO] Loading model sadop_xgb_model.joblib ...")
    clf = joblib.load(MODEL_PATH)

    print("[INFO] Loading raw_dataset.csv ...")
    df = pd.read_csv(DATA_PATH)

    if df.empty:
        print("[ERROR] raw_dataset.csv est vide, aucune requête à prédire.")
        return

    # On choisit comme exemple la requête la plus lente (avg_time_s max)
    df_sorted = df.sort_values("avg_time_s", ascending=False)
    sample = df_sorted.iloc[0]

    print("\n=== Sample used for prediction ===")
    print(f"DIGEST_TEXT: {sample['DIGEST_TEXT']}")
    print(f"avg_time_s: {sample['avg_time_s']}")
    print(f"exec_count: {sample['exec_count']}")
    print(f"rows_examined: {sample['rows_examined']}")

    # On reconstruit les features avec les colonnes utilisées par build_dataset.py
    # Catégorielles (issues de EXPLAIN)
    explain_type = sample.get("explain_type", "UNKNOWN")
    explain_key = sample.get("explain_key", "UNKNOWN")
    explain_extra = sample.get("explain_extra", "UNKNOWN")

    if pd.isna(explain_type):
        explain_type = "UNKNOWN"
    if pd.isna(explain_key):
        explain_key = "UNKNOWN"
    if pd.isna(explain_extra):
        explain_extra = "UNKNOWN"

    # Numériques
    exec_count = sample.get("exec_count", 0) or 0
    total_time_s = sample.get("total_time_s", 0) or 0
    avg_time_s = sample.get("avg_time_s", 0) or 0
    rows_examined = sample.get("rows_examined", 0) or 0
    rows_sent = sample.get("rows_sent", 0) or 0
    explain_rows = sample.get("explain_rows", 0)
    if pd.isna(explain_rows):
        explain_rows = 0

    features = {
        # colonnes catégorielles
        "explain_type": explain_type,
        "explain_key": explain_key,
        "explain_extra": explain_extra,
        # colonnes numériques
        "exec_count": exec_count,
        "total_time_s": total_time_s,
        "avg_time_s": avg_time_s,
        "rows_examined": rows_examined,
        "rows_sent": rows_sent,
        "explain_rows": explain_rows,
    }

    # On crée un DataFrame avec exactement les mêmes noms de colonnes
    X_sample = pd.DataFrame([features])

    print("\n[INFO] Running prediction ...\n")
    y_pred = clf.predict(X_sample)[0]

    # Si le modèle supporte predict_proba, on l'affiche (sinon, on ignore)
    try:
        proba = clf.predict_proba(X_sample)[0]
    except Exception:
        proba = None

    print("=== Prediction result ===")
    label_str = "SLOW QUERY" if y_pred == 1 else "FAST QUERY"
    print(f"Predicted label: {y_pred}  -> {label_str}")

    if proba is not None:
        print(f"Prediction probabilities: {proba}")


if __name__ == "__main__":
    main()

