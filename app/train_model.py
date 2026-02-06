import joblib
from sklearn.pipeline import Pipeline
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import classification_report
from xgboost import XGBClassifier

from build_dataset import prepare_train_test


def train():
    X_train, X_test, y_train, y_test, preprocessor = prepare_train_test()

    n_train = len(X_train)
    n_test = len(X_test)
    n_total = n_train + n_test
    n_classes = y_train.nunique()

    print(f"[INFO] Number of samples after cleaning: {n_total}")
    print(f"[INFO] Training samples: {n_train}, number of classes: {n_classes}")

    # Critère automatique de choix du modèle
    use_xgb = (n_train >= 50 and n_classes == 2)

    if use_xgb:
        print("[INFO] Using XGBoostClassifier (gradient boosting).")
        model = XGBClassifier(
            n_estimators=200,
            max_depth=6,
            learning_rate=0.1,
            subsample=0.8,
            colsample_bytree=0.8,
            objective="binary:logistic",
            eval_metric="logloss",
            tree_method="hist",
            random_state=42,
        )
    else:
        print("[WARN] Small dataset or not enough classes. Using LogisticRegression.")
        model = LogisticRegression(
            max_iter=2000,
            class_weight="balanced",
        )

    # Pipeline : préprocessing + modèle
    clf = Pipeline(
        steps=[
            ("preprocessor", preprocessor),
            ("clf", model),
        ]
    )

    # Entraînement
    clf.fit(X_train, y_train)

    # Évaluation
    y_pred = clf.predict(X_test)
    print(classification_report(y_test, y_pred))

    # Sauvegarde
    joblib.dump(clf, "sadop_xgb_model.joblib")
    print("[INFO] Saved sadop_xgb_model.joblib")


if __name__ == "__main__":
    train()