import pandas as pd
import joblib


MODEL_PATH = "sadop_xgb_model.joblib"
DATA_PATH = "raw_dataset.csv"


def main():
    # 1. Charger le modèle entraîné
    print("[INFO] Loading model sadop_xgb_model.joblib ...")
    clf = joblib.load(MODEL_PATH)

    # 2. Charger le dataset brut
    print("[INFO] Loading raw_dataset.csv ...")
    df = pd.read_csv(DATA_PATH)

    if df.empty:
        print("[ERROR] raw_dataset.csv is empty. Run collect_data.py first.")
        return

    # 3. Choisir une requête à prédire
    #    - ici, on prend la DERNIÈRE requête (la plus récente)
    #    - tu peux changer pour df.sample(1) si tu veux au hasard
    sample = df.iloc[-1]

    print("\n=== Sample used for prediction ===")
    print("DIGEST_TEXT:", sample["DIGEST_TEXT"])
    print("avg_time_s:", sample["avg_time_s"])
    print("exec_count:", sample["exec_count"])
    print("rows_examined:", sample["rows_examined"])

    # 4. Construire l'input pour le modèle
    #    IMPORTANT : les mêmes features que dans build_dataset.py
    X_input = pd.DataFrame(
        [
            {
                "DIGEST_TEXT": sample["DIGEST_TEXT"],
                "exec_count": sample["exec_count"],
                "total_time_s": sample["total_time_s"],
                "avg_time_s": sample["avg_time_s"],
                "rows_examined": sample["rows_examined"],
                "rows_sent": sample["rows_sent"],
                "explain_type": sample["explain_type"],
                "explain_key": sample["explain_key"],
                "explain_rows": sample["explain_rows"],
                "explain_extra": sample["explain_extra"],
            }
        ]
    )

    # 5. Prédiction
    print("\n[INFO] Running prediction ...\n")
    y_pred = clf.predict(X_input)[0]

    if hasattr(clf, "predict_proba"):
        proba = clf.predict_proba(X_input)[0]
    else:
        proba = None

    label_str = "SLOW QUERY" if int(y_pred) == 1 else "FAST QUERY"

    print("=== Prediction result ===")
    print(f"Predicted label: {y_pred}  -> {label_str}")
    if proba is not None and len(proba) == 2:
        print("Prediction probabilities:", proba)
    else:
        print("(No probability output available for this model.)")


if __name__ == "__main__":
    main()

