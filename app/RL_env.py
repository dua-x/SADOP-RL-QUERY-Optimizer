# RL_env.py
import time
import mysql.connector

from db import get_connection


def normalize_sql(sql: str) -> str:
    """
    Nettoie quelques patterns moches venant de DIGEST_TEXT :
    COUNT ( ... ) -> COUNT(...), etc.
    """
    patterns = ["COUNT", "SUM", "AVG", "MIN", "MAX"]
    for fn in patterns:
        sql = sql.replace(f"{fn} (", f"{fn}(")
        sql = sql.replace(f"{fn.lower()} (", f"{fn.lower()}(")
    return sql


class MySQLEnv:
    """
    Environnement RL pour optimiser des requêtes SQL via des actions (index).

    - On part d'une requête SQL donnée.
    - On lance EXPLAIN dessus pour voir quelles tables sont scannées.
    - À partir du plan, on génère dynamiquement une liste d'actions :

        self.actions = [
            {
                "table": None,
                "columns": [],
                "description": "Ne rien faire (aucun nouvel index)",
            },
            {
                "table": "users",
                "columns": ["city"],
                "description": "Créer un index sur users(city)",
            },
            ...
        ]

    - RL choisit ensuite parmi ces actions (0..n_actions-1).
    """

    def __init__(self, sql_query: str | None = None):
        if sql_query is None:
            sql_query = "SELECT 1"

        self.original_sql = sql_query.strip()
        self.sql_query = normalize_sql(self.original_sql)

        # état logique unique
        self.state = "slow_query"

        # actions construites dynamiquement d'après EXPLAIN
        explain_rows = self._get_explain_plan()
        self._build_actions_from_explain(explain_rows)

    # ------------------------------------------------------------------
    #   EXPLAIN
    # ------------------------------------------------------------------

    def _get_explain_plan(self):
        """
        Exécute EXPLAIN sur la requête. Retourne une liste de lignes (dict).
        Si EXPLAIN échoue, retourne [].
        """
        conn = get_connection()
        cursor = conn.cursor(dictionary=True)

        try:
            cursor.execute(f"EXPLAIN {self.sql_query}")
            rows = cursor.fetchall()
            print(f"[ENV] EXPLAIN returned {len(rows)} row(s).")
            return rows
        except mysql.connector.Error as e:
            print(f"[WARN] EXPLAIN failed for RL env: {e}")
            return []
        finally:
            cursor.close()
            conn.close()

    def _build_actions_from_explain(self, explain_rows, max_actions: int = 12):
        """
        Construit self.actions à partir du plan EXPLAIN.

        Idée :
          - On regarde les tables utilisées dans EXPLAIN.
          - Pour chaque table de ton schéma e-commerce, on propose plusieurs index possibles.
          - On évite les doublons (même table + mêmes colonnes).
          - On ajoute aussi des actions "utiliser l'index existant X" quand EXPLAIN indique un key.
        """
        # Action 0 : toujours "ne rien faire"
        actions = [
            {
                "table": None,
                "columns": [],
                "description": "Ne rien faire (aucun nouvel index)",
            }
        ]

        # Si EXPLAIN a échoué, on met juste quelques actions génériques
        if not explain_rows:
            print("[ENV] No EXPLAIN rows, using static default actions.")
            static_candidates = [
                ("users", ["city"]),
                ("users", ["age"]),
                ("users", ["city", "age"]),
                ("orders", ["user_id"]),
                ("orders", ["user_id", "status"]),
                ("orders", ["created_at"]),
                ("order_items", ["order_id"]),
                ("order_items", ["product_id"]),
                ("products", ["category"]),
                ("products", ["category", "price"]),
            ]
            for table, cols in static_candidates:
                if len(actions) >= max_actions:
                    break
                actions.append(
                    {
                        "table": table,
                        "columns": cols,
                        "description": f"Créer un index sur {table}({', '.join(cols)})",
                    }
                )
            self.actions = actions
            self.n_actions = len(actions)
            print("[ENV] RL actions (fallback):")
            for i, a in enumerate(self.actions):
                print(f"  - action {i}: {a['description']}")
            return

        # Sinon : EXPLAIN a retourné des lignes
        seen = set()

        for row in explain_rows:
            table = row.get("table")

            if not table:
                continue

            # On limite aux tables de ton schéma e-commerce
            if table not in ("users", "sessions", "orders", "order_items", "products"):
                continue

            # 🔹 1) Détection d’un index existant utilisé par MySQL
            key_used = row.get("key")  # nom de l'index utilisé dans cette étape du plan
            if key_used:
                key_sig = ("__existing__", key_used)
                if key_sig not in seen and len(actions) < max_actions:
                    seen.add(key_sig)
                    actions.append(
                        {
                            "table": None,
                            "columns": [],
                            "description": f"Utiliser l'index existant {key_used} (déjà présent)",
                            "existing_index": key_used,
                        }
                    )

            # 🔹 2) Propositions d'index candidats en fonction de la table
            if table == "users":
                candidates = [
                    ["city"],
                    ["age"],
                    ["city", "age"],
                ]
            elif table == "sessions":
                candidates = [
                    ["user_id"],
                    ["user_id", "created_at"],
                    ["session_type"],
                ]
            elif table == "orders":
                candidates = [
                    ["user_id"],
                    ["user_id", "status"],
                    ["created_at"],
                ]
            elif table == "order_items":
                candidates = [
                    ["order_id"],
                    ["product_id"],
                ]
            elif table == "products":
                candidates = [
                    ["category"],
                    ["category", "price"],
                ]
            else:
                candidates = []

            for cols in candidates:
                if len(actions) >= max_actions:
                    break

                key = (table, tuple(cols))
                if key in seen:
                    continue
                seen.add(key)

                actions.append(
                    {
                        "table": table,
                        "columns": cols,
                        "description": f"Créer un index sur {table}({', '.join(cols)})",
                    }
                )

        # Si malgré tout on n'a que action 0 → fallback générique
        if len(actions) == 1:
            print("[ENV] EXPLAIN ok mais aucun candidat trouvé, ajout de fallback générique.")
            fallback = [
                ("users", ["city"]),
                ("sessions", ["user_id"]),
                ("orders", ["user_id"]),
            ]
            for table, cols in fallback:
                actions.append(
                    {
                        "table": table,
                        "columns": cols,
                        "description": f"Créer un index sur {table}({', '.join(cols)})",
                    }
                )

        self.actions = actions
        self.n_actions = len(actions)

        print("[ENV] RL actions built from EXPLAIN:")
        for i, a in enumerate(self.actions):
            print(f"  - action {i}: {a['description']}")

    # ------------------------------------------------------------------
    #   API de base
    # ------------------------------------------------------------------

    def reset(self):
        """
        Réinitialise l'état (trivial ici).
        """
        self.state = "slow_query"
        return self.state

    def execute_query(self) -> float:
        """
        Exécute la requête SQL et retourne le temps réel d'exécution (en secondes).
        Si la requête échoue, on renvoie 10s (grosse pénalité).
        """
        conn = get_connection()
        cursor = conn.cursor()

        try:
            start = time.time()
            cursor.execute(self.sql_query)
            cursor.fetchall()
            end = time.time()
            duration = end - start
        except Exception as e:
            print(f"[WARN] Query execution failed: {e}")
            duration = 10.0
        finally:
            cursor.close()
            conn.close()

        return duration

    def measure_runtime(self) -> float:
        """
        Utilisée par RL_ML.train_rl_on_query pour mesurer
        un temps de référence AVANT actions.
        """
        return self.execute_query()

    # ------------------------------------------------------------------
    #   Actions (création d'index dynamiques)
    # ------------------------------------------------------------------

    def apply_action(self, action: int):
        """
        Applique une action d'indexation.

        0  : NO-OP
        1+ : on crée dynamiquement un index défini dans self.actions[action]
             ou, si c'est une action "existing_index", on ne crée rien et on explique.
        """
        if action == 0:
            return  # NO-OP

        if action < 0 or action >= self.n_actions:
            print(f"[WARN] Action {action} hors limites, aucun effet.")
            return

        entry = self.actions[action]
        table = entry.get("table")
        cols = entry.get("columns", [])
        desc = entry.get("description", f"Action {action}")

        # 🔹 Cas spécial : "index existant" détecté via EXPLAIN
        if "existing_index" in entry:
            idx_name = entry["existing_index"]
            print(f"[ENV] Action {action}: {desc}")
            print(f"[ENV] Aucun nouvel index créé. MySQL utilise déjà l'index {idx_name} pour cette requête.")
            return

        if not table or not cols:
            print(f"[WARN] Action {action} sans table/colonnes valides, skipping.")
            return

        index_name = f"idx_{table}_" + "_".join(cols)
        cols_sql = ", ".join(cols)

        conn = get_connection()
        cursor = conn.cursor()

        try:
            print(f"[ENV] Applying action {action}: {desc}")
            cursor.execute(f"CREATE INDEX {index_name} ON {table}({cols_sql})")
            conn.commit()
        except mysql.connector.Error as e:
            if e.errno == 1061:
                # index déjà existant
                print(f"[ENV] Index {index_name} already exists, skipping.")
            else:
                print(f"[WARN] Failed to apply action {action}: {e}")
        finally:
            cursor.close()
            conn.close()

    # ------------------------------------------------------------------
    #   Step RL
    # ------------------------------------------------------------------

    def step(self, action: int):
        """
        Étape RL :
          - mesure le temps AVANT
          - applique l'action (index éventuel)
          - mesure le temps APRÈS
          - reward = time_before - time_after
            => si time_after < time_before → reward positif (c’est bien)
        """
        time_before = self.execute_query()
        self.apply_action(action)
        time_after = self.execute_query()

        reward = time_before - time_after
        done = True  # un épisode = une action, pour simplifier

        info = {
            "time_before": time_before,
            "time_after": time_after,
        }

        return reward, done, info
