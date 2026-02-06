SADOP – RL Query Optimizer

Smart Agent for Database Optimization using Reinforcement Learning

SADOP is an experimental intelligent database assistant that analyzes SQL queries, detects performance issues, and automatically recommends index optimizations using Reinforcement Learning (Q-Learning) and MySQL EXPLAIN.

This project is intended for academic research and experimentation, particularly in:
 • Database Systems
 • Query Optimization
 • Reinforcement Learning
 • Intelligent Agents

⸻

✨ Key Features
 • Automatic SQL analysis using EXPLAIN
 • Reinforcement Learning agent (Q-Learning)
 • Dynamic index recommendation based on execution plans
 • MySQL real execution time measurement
 • Optional natural language interface (LLM-based)
 • Dockerized environment for easy deployment

⸻

How It Works
 1. The user provides a SQL query (directly or via natural language).
 2. SADOP runs EXPLAIN to analyze the execution plan.
 3. Candidate index actions are generated dynamically from the plan.
 4. A Reinforcement Learning agent:
 • Tries index actions
 • Measures execution time before and after
 • Learns which action improves performance
 5. SADOP returns a clear explanation:
 • Execution times
 • Performance gain or loss
 • Recommended index, or confirmation that no index is needed

⸻

📂 Project Structure

SADOP-RL-QUERY-Optimizer/
│
├── RL_env.py              # RL environment (EXPLAIN + dynamic actions)
├── RL_ML.py               # Q-Learning agent + training loop
├── sadop_nl_agent.py      # Natural language interface
├── db.py                  # MySQL connection helper
├── docker-compose.yml
├── requirements.txt
├── README.md
└── LICENSE


⸻

⚙️ Requirements

Recommended
 • Docker
 • Docker Compose

Without Docker
 • Python 3.9+
 • MySQL 8+
 • pip install -r requirements.txt

⸻

🐳 Quick Start (Docker)

1. Clone the repository

git clone https://github.com/dua-x/SADOP-RL-QUERY-Optimizer.git
cd SADOP-RL-QUERY-Optimizer

2. Configure environment variables

Create a .env file (do not commit it):

MYSQL_HOST=mysql
MYSQL_USER=root
MYSQL_PASSWORD=root
MYSQL_DATABASE=sadop_db

# Optional (natural language interface)
OPENAI_API_KEY=your_key_here

3. Start the containers

docker compose up -d

4. Enter the application container

docker exec -it sadop-app bash


⸻

Running SADOP

python sadop_nl_agent.py

Example query

SELECT u.city, COUNT(*) AS nb_users, AVG(s.duration) AS avg_duration
FROM users u
JOIN sessions s ON u.id = s.user_id
WHERE u.city LIKE '%a%'
AND s.created_at >= DATE_SUB(NOW(), INTERVAL 30 DAY)
GROUP BY u.city
HAVING nb_users > 100
ORDER BY avg_duration DESC;


⸻

📊 Example Output

[Résultat SADOP]
- Temps moyen AVANT : 0.0129 s
- Temps moyen APRÈS : 0.0079 s
- Gain global       : +4.99 ms
- Index recommandé  : Créer un index sur sessions(user_id)


⸻

Why Reinforcement Learning?

Unlike static optimizers, SADOP:
 • Learns from real query executions
 • Adapts to data distribution
 • Confirms when no index is the optimal choice

This closely reflects real-world DBA decision-making.

⸻

⚠️ Important Notes
 • This project creates indexes during experimentation.
 • Use a test database only, not production.
 • Some indexes (e.g. foreign keys) cannot be removed.

⸻

📜 License

This project is licensed under the MIT License.
Free to use, modify, and distribute for academic or personal use.

⸻

👨‍🎓 Academic Context

This project was developed as part of an academic assignment focused on:

Intelligent agents for database optimization using Reinforcement Learning.

⸻

Contributions

Contributions and improvements are welcome.
Issues and pull requests are encouraged.

⸻

⭐️ If you like this project

Give it a ⭐️ on GitHub — it helps a lot!
