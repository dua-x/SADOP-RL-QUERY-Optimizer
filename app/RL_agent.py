#RL_agent.py
import random

class QAgent:
    """
    Agent Q-learning ultra simple :
    - q_table[(state, action)] = valeur Q
    - epsilon-greedy pour choisir l'action
    """

    def __init__(self, actions=None, alpha=0.1, epsilon=0.2):
        # liste des actions possibles
        self.actions = actions if actions is not None else [0, 1, 2, 3]
        self.alpha = alpha      # taux d'apprentissage
        self.epsilon = epsilon  # probabilité d'exploration
        self.q_table = {}       # (state, action) -> Q

    def get_q(self, state, action):
        return self.q_table.get((state, action), 0.0)

    def choose_action(self, state):
        """
        Politique ε-greedy :
        - avec proba epsilon, on choisit une action au hasard (exploration)
        - sinon, on choisit l'action avec la meilleure valeur Q (exploitation)
        """
        if random.random() < self.epsilon:
            return random.choice(self.actions)

        # exploitation
        q_values = [self.get_q(state, a) for a in self.actions]
        max_q = max(q_values)
        # en cas d'égalité, on prend la première
        best_index = q_values.index(max_q)
        return self.actions[best_index]

    def update(self, state, action, reward):
        """
        Mise à jour Q-learning simplifiée :
        Q(s,a) ← Q(s,a) + α * (reward - Q(s,a))
        (on ne regarde pas l'état futur, donc c'est une forme "bandit")
        """
        old_q = self.get_q(state, action)
        new_q = old_q + self.alpha * (reward - old_q)
        self.q_table[(state, action)] = new_q
