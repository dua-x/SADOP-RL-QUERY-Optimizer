# RL_ML.py
import textwrap
from typing import Dict, Tuple, List, Any

from RL_env import MySQLEnv


# ============================================================
# 1. Agent Q-Learning tabulaire (intelligence = apprentissage)
# ============================================================

class QLearningAgent:
    """
    Agent Q-learning générique :
      - q_table[(state, action)] = valeur Q
      - epsilon-greedy pour choisir l'action
      - n_actions = len(env.actions) (dynamique)
    """

    def __init__(
        self,
        n_actions: int,
        alpha: float = 0.5,
        gamma: float = 0.9,
        epsilon: float = 0.3,
    ):
        """
        n_actions : nombre d'actions possibles (doit matcher len(env.actions))
        alpha     : learning rate
        gamma     : discount factor
        epsilon   : proba d'exploration (epsilon-greedy)
        """
        self.n_actions = n_actions
        self.alpha = alpha
        self.gamma = gamma
        self.epsilon = epsilon

        # Q-table : (state, action) -> Q
        self.Q: Dict[Tuple[str, int], float] = {}

    # ---------- helpers Q ---------- #

    def get_Q(self, state: str, action: int) -> float:
        return self.Q.get((state, action), 0.0)

    def set_Q(self, state: str, action: int, value: float):
        self.Q[(state, action)] = value

    # ---------- policy (epsilon-greedy) ---------- #

    def choose_action(self, state: str) -> int:
        """
        Politique epsilon-greedy :
        - avec proba epsilon -> action aléatoire (exploration)
        - sinon -> argmax_a Q(s,a) (exploitation)
        """
        import random

        # Exploration
        if random.random() < self.epsilon:
            return random.randint(0, self.n_actions - 1)

        # Exploitation
        best_a = 0
        best_q = self.get_Q(state, 0)

        for a in range(1, self.n_actions):
            q = self.get_Q(state, a)
            if q > best_q:
                best_q = q
                best_a = a

        return best_a

    def update(self, state: str, action: int, reward: float, next_state: str):
        """
        Q-learning standard :
          Q(s,a) <- Q(s,a) + alpha * (reward + gamma * max_a' Q(s',a') - Q(s,a))
        """
        old_q = self.get_Q(state, action)
        max_next_q = max(self.get_Q(next_state, a) for a in range(self.n_actions))
        new_q = old_q + self.alpha * (reward + self.gamma * max_next_q - old_q)
        self.set_Q(state, action, new_q)


# ============================================================
# 2. Entraînement RL sur UNE requête (actions dynamiques)
# ============================================================

def train_rl_on_query(
    agent: QLearningAgent,
    sql_query: str,
    n_episodes: int = 4,
) -> Dict[str, Any]:
    """
    Entraîne l'agent sur UNE requête SQL et renvoie un résumé structuré.

    Retourne un dict du type :
    {
        "sql": ...,
        "initial_time": ...,
        "avg_time_after": ...,
        "global_gain": ...,
        "best_action": int,
        "best_desc": str,
        "episodes": [...]
    }
    """
    env = MySQLEnv(sql_query)

    # On récupère les descriptions d'actions depuis l'env
    # env.actions est une liste de dicts :
    #   { "table": ..., "columns": [...], "description": "..." }
    action_desc = {
        i: a.get("description", f"Action {i}")
        for i, a in enumerate(env.actions)
    }

    print("\n==================== RL OPTIMIZATION ====================")
    print(textwrap.fill(sql_query.strip(), width=100))
    print("=========================================================\n")

    state = env.reset()

    # Temps de référence AVANT RL
    initial_time = env.measure_runtime()
    print(f"[INFO] Temps moyen initial de la requête : {initial_time:.4f} s\n")

    episodes_info: List[Dict[str, Any]] = []

    for ep in range(1, n_episodes + 1):
        # Choix de l'action (en utilisant la Q-table)
        action = agent.choose_action(state)

        # Step environnement : applique l'action (index) + mesure temps
        reward, done, info = env.step(action)

        ep_info = {
            "episode": ep,
            "action": action,
            "time_before": info["time_before"],
            "time_after": info["time_after"],
            "reward": reward,
        }
        episodes_info.append(ep_info)

        # Mise à jour Q(s,a)
        next_state = state  # état unique "slow_query"
        agent.update(state, action, reward, next_state)

        desc = action_desc.get(action, f"Action {action}")
        delta = info["time_before"] - info["time_after"]
        delta_ms = delta * 1000

        print(
            f"👉 Épisode {ep:02d} : {desc}\n"
            f"   - Temps AVANT : {info['time_before']:.4f} s\n"
            f"   - Temps APRÈS : {info['time_after']:.4f} s\n"
            f"   - Gain (reward) : {reward:.4f} s ({delta_ms:+.2f} ms)\n"
        )

        if done:
            state = env.reset()

    # Si aucune info (bizarre, mais on sécurise)
    if not episodes_info:
        return {
            "sql": sql_query,
            "initial_time": initial_time,
            "avg_time_after": initial_time,
            "global_gain": 0.0,
            "best_action": 0,
            "best_desc": action_desc.get(0, "Ne rien faire (aucun nouvel index)"),
            "episodes": [],
        }

    # épisode avec le meilleur reward
    best_ep = max(episodes_info, key=lambda x: x["reward"])
    best_action = best_ep["action"]
    best_desc = action_desc.get(best_action, f"Action {best_action}")

    avg_time_after = sum(ep["time_after"] for ep in episodes_info) / len(episodes_info)
    global_gain = initial_time - avg_time_after

    print("=============== RÉSUMÉ OPTIMISATION RL ===============")
    print(f"- Temps moyen initial : {initial_time:.4f} s")
    print(f"- Temps moyen après RL : {avg_time_after:.4f} s")
    print(f"- Gain global : {global_gain:.4f} s ({global_gain*1000:.2f} ms)\n")
    print(f"- Action la plus prometteuse : {best_desc}")
    print("=======================================================\n")

    return {
        "sql": sql_query,
        "initial_time": initial_time,
        "avg_time_after": avg_time_after,
        "global_gain": global_gain,
        "best_action": best_action,
        "best_desc": best_desc,
        "episodes": episodes_info,
    }

# Petit main de test local (optionnel)
if __name__ == "__main__":
    sql = "SELECT * FROM users WHERE city LIKE '%a%'"
    # On crée un env pour connaître le nombre d'actions dynamiques
    env = MySQLEnv(sql)
    agent = QLearningAgent(n_actions=len(env.actions))
    train_rl_on_query(agent, sql_query=sql, n_episodes=3)
