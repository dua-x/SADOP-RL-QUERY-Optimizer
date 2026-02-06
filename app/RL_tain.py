#RL_tain.py
from app.RL_env import ACTION_DESCRIPTIONS ,MySQLEnv
from RL_agent import QAgent

query = """
SELECT u.city, AVG(s.duration)
FROM users_small u
JOIN sessions_small s ON u.id = s.user_id
GROUP BY u.city
"""

agent = QAgent(actions=list(ACTION_DESCRIPTIONS.keys()))

for episode in range(10):
    env = MySQLEnv(query)
    state = "slow_query"

    action = agent.choose_action(state)
    reward, _ = env.step(action)
    agent.update(state, action, reward)

    print(f"Episode {episode} | Action={action} | Reward={reward:.3f}")
