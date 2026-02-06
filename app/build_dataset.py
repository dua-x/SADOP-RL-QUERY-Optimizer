import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
import joblib

# Fichier d'entrée produit par collect_data.py
INPUT_CSV = "raw_dataset.csv"


def prepare_train_test(test_size: float = 0.2, random_state: int = 42):
    """
    Charge raw_dataset.csv, nettoie les données, prépare X et y,
    construit le préprocesseur pour les features, et retourne :
    X_train, X_test, y_train, y_test, preprocessor
    """
    # 1) Charger le dataset brut
    df = pd.read_csv(INPUT_CSV)

    # On enlève les lignes sans label
    df = df.dropna(subset=["label_slow"])

    # 2) Colonnes EXPLAIN catégorielles (on force UNKNOWN si absent ou NaN)
    cat_cols = []
    for col in ["explain_type", "explain_key", "explain_extra"]:
        if col in df.columns:
            df[col] = df[col].fillna("UNKNOWN")
        else:
            df[col] = "UNKNOWN"
        cat_cols.append(col)

    # 3) Colonnes numériques
    num_cols = []
    for col in ["exec_count", "total_time_s", "avg_time_s",
                "rows_examined", "rows_sent", "explain_rows"]:
        if col in df.columns:
            num_cols.append(col)

    # Remplacer les NaN numériques par 0
    if num_cols:
        df[num_cols] = df[num_cols].fillna(0)

    # 4) Construire X (features) et y (label)
    y = df["label_slow"].astype(int)
    X = df[cat_cols + num_cols]

    n_samples = len(df)
    n_classes = y.nunique()
    print(f"[INFO] Number of samples after cleaning: {n_samples}")
    print(f"[INFO] Number of classes: {n_classes}")

    # 5) Préprocesseur : OneHot pour les cat, passthrough pour les num
    preprocessor = ColumnTransformer(
        transformers=[
            ("cat", OneHotEncoder(handle_unknown="ignore"), cat_cols),
            ("num", "passthrough", num_cols),
        ]
    )

    # 6) Si dataset très petit ou une seule classe → mode démo
    if n_samples < 5 or n_classes < 2:
        print("[WARN] Very small dataset or single class, "
              "using all samples for both train and test (demo mode).")
        X_train = X.copy()
        X_test = X.copy()
        y_train = y.copy()
        y_test = y.copy()
        return X_train, X_test, y_train, y_test, preprocessor

    # 7) Split train / test avec stratification si possible
    try:
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=random_state,
            stratify=y if n_classes > 1 else None,
        )
    except ValueError as e:
        print(f"[WARN] train_test_split failed with stratify: {e}")
        X_train, X_test, y_train, y_test = train_test_split(
            X,
            y,
            test_size=test_size,
            random_state=random_state,
        )

    return X_train, X_test, y_train, y_test, preprocessor


if __name__ == "__main__":
    # Pour lancer directement : prépare les données et sauvegarde le préprocesseur
    X_train, X_test, y_train, y_test, preprocessor = prepare_train_test()
    preprocessor.fit(X_train)
    joblib.dump(preprocessor, "preprocessor.joblib")
    print("Saved preprocessor.joblib")
