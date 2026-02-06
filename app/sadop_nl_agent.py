"""
sadop_nl_agent.py

Agent d'administration SADOP en langage naturel.
Version avec VRAI LLM (OpenAI ou équivalent) pour :

  - comprendre les demandes en langage naturel
  - router vers les bonnes actions (ML, RL, MySQL)
  - éventuellement générer du SQL à partir du langage naturel

Le LLM joue le rôle d'orchestrateur, "LangChain-like" :
  - il lit le texte utilisateur
  - il renvoie un JSON structuré avec intent_type / sql / top_k
  - le code Python exécute ensuite les fonctions adaptées.

Intents possibles :
  - DIAGNOSE_SQL          : diagnostiquer une requête donnée
  - SHOW_TOP_SLOW_QUERIES: afficher les requêtes lentes connues (raw_dataset.csv)
  - OPTIMIZE_WORST_QUERY : lancer RL sur la plus lente
  - OPTIMIZE_GIVEN_SQL   : lancer RL sur une requête donnée
  - SHOW_HELP            : afficher l'aide
  - UNKNOWN              : rien de clair

Nécessite :
  - sadop_xgb_model.joblib
  - raw_dataset.csv
  - RL_ML.py (QLearningAgent, train_rl_on_query)
  - db.py  (get_connection)

Optionnel :
  - pip install openai
  - export OPENAI_API_KEY="ta_cle"
"""

import os
from dotenv import load_dotenv
import re
import json
import time
import textwrap
from dataclasses import dataclass
from typing import Optional, Literal, Dict, Any

import joblib
import pandas as pd
import mysql.connector
from RL_agent import QAgent
from RL_env import MySQLEnv
from db import get_connection
load_dotenv()  # Charger les variables d'environnement depuis .env
# =========================================================
#  LLM (OpenAI) - facultatif, avec fallback automatique
# =========================================================

try:
    from openai import OpenAI
    _api_key = os.getenv("OPENAI_API_KEY")

    if _api_key:
        client = OpenAI(
            api_key=_api_key,
            base_url="https://api.groq.com/openai/v1"
            )
        LLM_AVAILABLE = True
    else:
        client = None
        LLM_AVAILABLE = False
except Exception as e:
    client = None
    LLM_AVAILABLE = False
    LLM_IMPORT_ERROR = e

# =========================================================
#  RL (Q-Learning) - facultatif aussi
# =========================================================

try:
    from RL_ML import QLearningAgent, train_rl_on_query
    RL_AVAILABLE = True
except Exception:
    RL_AVAILABLE = False


# ================== 1. TYPES DE BASE ================== #

IntentType = Literal[
    "DIAGNOSE_SQL",          # diagnostiquer une requête donnée
    "SHOW_TOP_SLOW_QUERIES", # afficher les requêtes lentes connues
    "OPTIMIZE_WORST_QUERY",  # lancer RL sur la plus lente
    "OPTIMIZE_GIVEN_SQL",    # lancer RL sur une requête donnée
    "SHOW_HELP",
    "UNKNOWN",
]


@dataclass
class Intent:
    type: IntentType
    sql: Optional[str] = None   # quand l'utilisateur donne lui-même un SELECT
    top_k: int = 5              # nombre de requêtes à afficher pour certains intents


MODEL_PATH = "sadop_xgb_model.joblib"


# ================== 2. PROMPT SYSTÈME LLM ================== #

INTENT_SYSTEM_PROMPT = """
Tu es SADOP, un agent d'administration MySQL intelligent.
Ton but est de lire la phrase de l'utilisateur (en français ou en anglais)
et de renvoyer une réponse STRICTEMENT au format JSON, SANS texte autour.

Tu DOIS renvoyer un JSON avec exactement les clés suivantes :

{
  "intent_type": "...",
  "sql": "... ou null",
  "top_k": 5
}

Les valeurs possibles pour "intent_type" sont :
- "DIAGNOSE_SQL"          : l'utilisateur veut diagnostiquer une requête SQL précise.
- "SHOW_TOP_SLOW_QUERIES" : l'utilisateur veut voir les requêtes les plus lentes (historique).
- "OPTIMIZE_WORST_QUERY"  : l'utilisateur veut optimiser automatiquement la ou les requêtes les plus lentes.
- "OPTIMIZE_GIVEN_SQL"    : l'utilisateur veut que tu optimises une requête SQL précise (avec RL + index).
- "SHOW_HELP"             : l'utilisateur demande de l'aide ("aide", "help", etc.).
- "UNKNOWN"               : tu ne comprends pas.

Règles :

1) Si le texte contient une requête SQL (SELECT, select, etc.),
   tu dois essayer d'extraire cette requête dans "sql" (en gardant le SQL brut).

2) Si l'utilisateur dit "diagnostiquer", "analyse", "analyse la performance de ...",
   et qu'il y a un SELECT, tu mets :
   "intent_type": "DIAGNOSE_SQL"

3) Si l'utilisateur dit "optimise cette requête", "optimize this query",
   et qu'il y a un SELECT, tu mets :
   "intent_type": "OPTIMIZE_GIVEN_SQL"

4) Si l'utilisateur demande "top 5 requêtes lentes", "montre les 10 requêtes les plus lentes",
   "liste les requêtes lentes", etc., tu mets :
   "intent_type": "SHOW_TOP_SLOW_QUERIES"
   Et "top_k": le nombre demandé (sinon 5 par défaut).

5) Si l'utilisateur dit "optimise la requête la plus lente",
   "optimise les top 3 requêtes lentes", etc., tu mets :
   "intent_type": "OPTIMIZE_WORST_QUERY"
   Et "top_k": nombre demandé (sinon 1 par défaut).

6) Si l'utilisateur dit juste "aide", "help", "que puis-je faire ?",
   tu mets :
   "intent_type": "SHOW_HELP"

7) Si tu ne comprends pas du tout, mets :
   "intent_type": "UNKNOWN"
   "sql": null
   "top_k": 5

IMPORTANT :
- Tu ne dois PAS exécuter de SQL, seulement le renvoyer.
- Tu ne dois pas inventer de champs ou de clés supplémentaires dans le JSON.
- Réponds UNIQUEMENT par ce JSON, sans phrase avant ou après.
"""


# ================== 3. ROUTEUR LLM + HEURISTIQUE ================== #

def extract_sql_from_prompt(prompt: str) -> Optional[str]:
    """
    Heuristique simple pour extraire un SELECT depuis le texte utilisateur.
    """
    match = re.search(r"(SELECT|select)\b", prompt, re.IGNORECASE)
    if match:
        return prompt[match.start():].strip()
    return None


def heuristic_detect_intent(prompt: str) -> Intent:
    """
    Version de secours si le LLM n'est pas dispo.
    (approx ce que tu avais avant)
    """
    p = prompt.lower()
    sql_in_text = extract_sql_from_prompt(prompt)
    top_k = 5

    if "aide" in p or "help" in p:
        return Intent(type="SHOW_HELP")

    # "top 10 requêtes lentes" etc.
    if "top" in p and ("requêtes lentes" in p or "slow queries" in p):
        m = re.search(r"top\s+(\d+)", p)
        if m:
            try:
                top_k = int(m.group(1))
            except ValueError:
                top_k = 5
        return Intent(type="SHOW_TOP_SLOW_QUERIES", top_k=top_k)

    # "requête la plus lente"
    if "requête la plus lente" in p or "plus lente" in p or "worst query" in p:
        return Intent(type="OPTIMIZE_WORST_QUERY", top_k=1)

    # "optimise cette requête : SELECT ..."
    if ("optimise" in p or "optimiser" in p or "optimize" in p) and sql_in_text:
        return Intent(type="OPTIMIZE_GIVEN_SQL", sql=sql_in_text)

    # "diagnostiquer cette requête : SELECT ..."
    if ("diagnostiquer" in p or "analyse" in p or "diagnose" in p) and sql_in_text:
        return Intent(type="DIAGNOSE_SQL", sql=sql_in_text)

    if "requêtes lentes" in p or "slow queries" in p:
        return Intent(type="SHOW_TOP_SLOW_QUERIES", top_k=top_k)

    return Intent(type="UNKNOWN")


def llm_route_intent(prompt: str) -> Intent:
    """
    Essaie d'utiliser un LLM OpenAI pour comprendre la demande.
    Si le LLM n'est pas dispo (pas de clé, pas de lib, pas de réseau),
    on retombe sur heuristic_detect_intent().
    """
    if not LLM_AVAILABLE or client is None:
        # Fallback : heuristique maison
        return heuristic_detect_intent(prompt)

    try:
        resp = client.chat.completions.create(
            model="llama-3.1-8b-instant",
            messages=[
                {"role": "system", "content": INTENT_SYSTEM_PROMPT},
                {"role": "user", "content": prompt},
            ],
            temperature=0,
        )

        # Texte brut renvoyé par le modèle (doit être un JSON)
        content = resp.choices[0].message.content

        data = json.loads(content)

        t = data.get("intent_type", "UNKNOWN")
        sql = data.get("sql") or None
        top_k = int(data.get("top_k", 5))

        if t not in (
            "DIAGNOSE_SQL",
            "SHOW_TOP_SLOW_QUERIES",
            "OPTIMIZE_WORST_QUERY",
            "OPTIMIZE_GIVEN_SQL",
            "SHOW_HELP",
            "UNKNOWN",
        ):
            t = "UNKNOWN"

        return Intent(type=t, sql=sql, top_k=top_k)

    except Exception as e:
        # En cas d'erreur (pas de réseau, mauvais JSON, etc.) on repasse en heuristique
        print(f"[WARN] LLM routing failed ({e}), fallback to heuristic_detect_intent().")
        return heuristic_detect_intent(prompt)


# ================== 4. UTILITAIRES ML & DB ================== #

def load_model():
    clf = joblib.load(MODEL_PATH)
    return clf


def explain_query(sql: str) -> Dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor(dictionary=True)
    try:
        cursor.execute(f"EXPLAIN {sql}")
        rows = cursor.fetchall()
        if not rows:
            return {
                "explain_type": "UNKNOWN",
                "explain_key": "UNKNOWN",
                "explain_rows": 0,
                "explain_extra": None,
            }
        row = rows[0]
        return {
            "explain_type": row.get("type", "UNKNOWN"),
            "explain_key": row.get("key", "UNKNOWN"),
            "explain_rows": row.get("rows", 0),
            "explain_extra": row.get("Extra", None),
        }
    finally:
        cursor.close()
        conn.close()


def measure_runtime_and_rows(sql: str, n_runs: int = 3) -> Dict[str, Any]:
    conn = get_connection()
    cursor = conn.cursor()
    times = []
    rows_sent = 0

    for _ in range(n_runs):
        try:
            start = time.time()
            cursor.execute(sql)
            results = cursor.fetchall()
            end = time.time()
            times.append(end - start)
            rows_sent = len(results)
        except Exception as e:
            print(f"[WARN] Query execution failed: {e}")
            times.append(10.0)
            rows_sent = 0

    cursor.close()
    conn.close()

    avg_time_s = sum(times) / len(times)
    rows_examined = rows_sent  # approximation

    return {
        "avg_time_s": avg_time_s,
        "rows_examined": rows_examined,
        "rows_sent": rows_sent,
    }


def build_feature_row_for_model(sql: str) -> pd.DataFrame:
    explain_info = explain_query(sql)
    runtime_info = measure_runtime_and_rows(sql, n_runs=3)

    avg_time_s = runtime_info["avg_time_s"]
    rows_examined = runtime_info["rows_examined"]
    rows_sent = runtime_info["rows_sent"]

    record = {
        "DIGEST_TEXT": sql,
        "exec_count": 1,
        "total_time_s": avg_time_s,
        "avg_time_s": avg_time_s,
        "rows_examined": rows_examined,
        "rows_sent": rows_sent,
        "explain_type": explain_info["explain_type"],
        "explain_key": explain_info["explain_key"],
        "explain_rows": explain_info["explain_rows"],
        "explain_extra": explain_info["explain_extra"],
    }

    df = pd.DataFrame([record])
    return df


def predict_slow_probability(sql: str, clf) -> Dict[str, Any]:
    df = build_feature_row_for_model(sql)
    X = df.drop(columns=["DIGEST_TEXT"], errors="ignore")
    proba = clf.predict_proba(X)[0]
    label = int(proba[1] >= 0.5)
    return {
        "label": label,
        "p_fast": float(proba[0]),
        "p_slow": float(proba[1]),
        "features": df,
    }


def get_top_slow_queries_from_dataset(k: int = 5) -> pd.DataFrame:
    df = pd.read_csv("raw_dataset.csv")
    df = df.dropna(subset=["avg_time_s"])
    df_sorted = df.sort_values(by="avg_time_s", ascending=False)
    return df_sorted.head(k)


# ================== 5. ACTIONS HAUT NIVEAU ================== #

def handle_diagnose_sql(sql: str, clf):
    print("\n========== DIAGNOSTIC ML ==========")
    print(sql)
    print("===================================\n")

    runtime_info = measure_runtime_and_rows(sql, n_runs=3)
    print(f"[REAL] avg_time_s     : {runtime_info['avg_time_s']:.6f}")
    print(f"[REAL] rows_examined  : {runtime_info['rows_examined']}")
    print(f"[REAL] rows_sent      : {runtime_info['rows_sent']}")

    pred = predict_slow_probability(sql, clf)
    label = "SLOW" if pred["label"] == 1 else "FAST"
    print("\n[ML] Prediction : ", label)
    print(f"[ML] P(fast)= {pred['p_fast']:.4f} | P(slow)= {pred['p_slow']:.4f}\n")


def handle_show_top_slow_queries(top_k: int):
    df_top = get_top_slow_queries_from_dataset(k=top_k)

    print(f"\n===== TOP {top_k} REQUÊTES LENTES (données historiques) =====")
    for i, row in df_top.iterrows():
        print(f"\n--- #{i} ---")
        print(f"avg_time_s : {row['avg_time_s']:.6f}")
        print(f"exec_count : {row['exec_count']}")
        print("SQL        :")
        print(textwrap.fill(row['DIGEST_TEXT'], width=100))
    print("============================================================\n")


def handle_optimize_worst_query(clf, top_k: int = 1, n_episodes: int = 4):
    if not RL_AVAILABLE:
        print("[WARN] RL non disponible (impossible d'importer RL_ML).")
        return

    df_top = get_top_slow_queries_from_dataset(k=top_k)

    for idx, row in df_top.iterrows():
        sql = row["DIGEST_TEXT"]
        probe_env = MySQLEnv(sql)
        n_actions = len(probe_env.actions)
        agent = QLearningAgent(n_actions=n_actions)

        print("\n===================================================")
        print(f"[RL] Optimisation de la requête #{idx} :")
        print(textwrap.fill(sql, width=100))
        print("===================================================")

        summary = train_rl_on_query(agent, sql, n_episodes=n_episodes)
        gain_ms = summary["global_gain"] * 1000
        print("\n[Résultat SADOP - Optimisation de ta requête]")
        print("----------------------------------------------")
        print("Ta requête :")
        print(textwrap.fill(summary["sql"].strip(), width=100))
        print()
        print(f"- Temps moyen AVANT optimisation : {summary['initial_time']:.4f} s")
        print(f"- Temps moyen APRÈS optimisation : {summary['avg_time_after']:.4f} s")
        print(f"- Gain global estimé            : {summary['global_gain']:.4f} s ({gain_ms:.2f} ms)")
        print(f"- Index recommandé              : {summary['best_desc']}")
        print("----------------------------------------------\n")



def handle_optimize_given_sql(sql: str, n_episodes: int = 4):
    if not RL_AVAILABLE:
        print("[WARN] RL non disponible (impossible d'importer RL_ML).")
        return

    # 1) On crée un env pour connaître le nombre d'actions dynamiques
    env = MySQLEnv(sql)
    agent = QLearningAgent(n_actions=len(env.actions))

    # 2) On relance un entraînement RL “complet” (train_rl_on_query recrée son env)
    summary = train_rl_on_query(agent, sql, n_episodes=n_episodes)

    gain_ms = summary["global_gain"] * 1000
    print("\n[Résultat SADOP - Optimisation de ta requête]")
    print("----------------------------------------------")
    print("Ta requête :")
    print(textwrap.fill(summary["sql"].strip(), width=100))
    print()
    print(f"- Temps moyen AVANT optimisation : {summary['initial_time']:.4f} s")
    print(f"- Temps moyen APRÈS optimisation : {summary['avg_time_after']:.4f} s")
    print(f"- Gain global estimé            : {summary['global_gain']:.4f} s ({gain_ms:.2f} ms)")
    print(f"- Index recommandé              : {summary['best_desc']}")
    print("----------------------------------------------\n")

    
def show_help():
    print(
        textwrap.dedent(
            """
        ===================== AIDE SADOP =====================

        Exemples de commandes en langage naturel :

        1) Diagnostiquer une requête précise
           - "diagnostiquer cette requête : SELECT ... "
           - "analyse la performance de : SELECT u.city, AVG(...) ..."

        2) Voir les requêtes les plus lentes (historique MySQL)
           - "montre les requêtes lentes"
           - "affiche les top 10 slow queries"
           - "quelles sont les requêtes les plus lentes ?"

        3) Optimiser automatiquement les pires requêtes (RL + index)
           - "optimise la requête la plus lente"
           - "optimise les top 3 requêtes lentes"
           - "optimise cette requête : SELECT ..."

        4) Aide
           - "aide"
           - "help"
           - "que puis-je faire ?"

        Le système combine :
           - ML (XGBoost) pour prédire si une requête est lente
           - RL (Q-Learning) pour proposer des index à créer
           - LLM pour comprendre le langage naturel et router les actions
           - Analyse MySQL (EXPLAIN, temps d'exécution réel)

        =====================================================
        """
        )
    )


# ================== 6. BOUCLE PRINCIPALE ================== #

def main():
    print("\n=== SADOP Natural Language Agent (LLM + ML + RL + MySQL) ===\n")
    print("Tape 'quit' ou 'exit' pour quitter, 'aide' pour afficher l'aide.\n")

    try:
        clf = load_model()
        print("[INFO] Modèle ML chargé avec succès.")
    except Exception as e:
        print(f"[WARN] Impossible de charger le modèle ML ({e}).")
        clf = None

    if LLM_AVAILABLE:
        print("[INFO] LLM OpenAI disponible (clé détectée).")
    else:
        print(
            "[WARN] LLM non disponible (lib openai absente ou pas de clé / pas de réseau). "
            "Utilisation du routeur heuristique interne."
        )

    while True:
        try:
            prompt = input("SADOP> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n[INFO] Bye.")
            break

        if not prompt:
            continue

        if prompt.lower() in ("quit", "exit"):
            print("[INFO] Fin de session.")
            break

        # 1) On laisse le LLM (ou l'heuristique) décider de l'intent
        intent = llm_route_intent(prompt)

        # 2) On exécute l'action associée
        if intent.type == "SHOW_HELP":
            show_help()
            continue

        if intent.type == "UNKNOWN":
            print("[WARN] Je n'ai pas compris la demande. Tape 'aide' pour des exemples.")
            continue

        if intent.type == "SHOW_TOP_SLOW_QUERIES":
            handle_show_top_slow_queries(intent.top_k)
            continue

        if intent.type == "DIAGNOSE_SQL":
            if not clf:
                print("[WARN] Modèle ML non disponible, diagnostic ML impossible.")
            else:
                if not intent.sql:
                    print("[WARN] Aucune requête SQL détectée dans ton message.")
                else:
                    handle_diagnose_sql(intent.sql, clf)
            continue

        if intent.type == "OPTIMIZE_WORST_QUERY":
            if not clf:
                print("[WARN] Modèle ML non disponible, mais on peut quand même lancer RL.")
            handle_optimize_worst_query(clf, top_k=intent.top_k, n_episodes=4)
            continue

        if intent.type == "OPTIMIZE_GIVEN_SQL":
            if not intent.sql:
                print("[WARN] Je n'ai pas trouvé de requête SQL à optimiser.")
            else:
                handle_optimize_given_sql(intent.sql, n_episodes=4)
            continue


if __name__ == "__main__":
    main()

