import random
import time
import mysql.connector
from db import get_connection

# ==========================
# 0) CONSTANTES COMMUNES
# ==========================

CITIES = [
    "Paris", "Lyon", "Marseille", "Lille",
    "Nice", "Toulouse", "Mulhouse", "Bordeaux", "Strasbourg"
]

SESSION_TYPES = ["web", "mobile", "cardio", "muscu", "hiit"]
DEVICES = ["desktop", "android", "ios", "tablet"]
ORDER_STATUSES = ["PENDING", "PAID", "SHIPPED"]


# ==========================
# 1) QUERIES PLUTÔT RAPIDES
# ==========================

def get_fast_queries():
    """
    Requêtes qui exploitent bien les index, avec LIMIT.
    Celles-ci devraient être plus "rapides" en moyenne.
    """
    return [
        # users : filtrage sur city + LIMIT avec index idx_users_city
        "SELECT * FROM users WHERE city = '{city}' ORDER BY id DESC LIMIT 50",

        # users : filtrage sur age + created_at (index sur created_at)
        """
        SELECT id, name, age, city
        FROM users
        WHERE age BETWEEN {age_min} AND {age_max}
        ORDER BY created_at DESC
        LIMIT 100
        """,

        # sessions filtrées sur user_id (index) + LIMIT
        """
        SELECT *
        FROM sessions
        WHERE user_id = {user_id}
        ORDER BY created_at DESC
        LIMIT 100
        """,

        # produit par catégorie (index idx_products_category)
        """
        SELECT id, name, price
        FROM products
        WHERE category = '{category}'
        ORDER BY price ASC
        LIMIT 50
        """,

        # orders par user (index idx_orders_user_id)
        """
        SELECT *
        FROM orders
        WHERE user_id = {user_id}
        ORDER BY created_at DESC
        LIMIT 50
        """,

        # petit join optimisé : users + sessions avec filtre sur city + LIMIT
        """
        SELECT u.id, u.city, s.duration
        FROM users u
        JOIN sessions s ON u.id = s.user_id
        WHERE u.city = '{city}'
        ORDER BY s.duration DESC
        LIMIT 100
        """,

        # stats simple sur sessions (GROUP BY sur colonne indexée)
        """
        SELECT session_type, COUNT(*) AS nb
        FROM sessions
        GROUP BY session_type
        """
    ]


# ==========================
# 2) QUERIES LENTES (TEMPLATES)
# ==========================

def get_slow_templates():
    """
    Requêtes volontairement "mauvaises" :
    - LIKE '%...%' → impossible d'utiliser l'index
    - fonctions dans WHERE (LOWER(), DATE())
    - gros GROUP BY sans LIMIT
    - joins larges
    """
    return [
        # 1) LIKE %xxx% sur une colonne indexée → casse l'index
        """
        SELECT *
        FROM users
        WHERE name LIKE '%{substr}%'
        ORDER BY created_at DESC
        """,

        # 2) LOWER(city) = ... → casse l'index sur city
        """
        SELECT *
        FROM users
        WHERE LOWER(city) = LOWER('{city}')
        ORDER BY id DESC
        """,

        # 3) DATE(created_at) = ... sur sessions → casse index created_at
        """
        SELECT *
        FROM sessions
        WHERE DATE(created_at) = DATE('{date_str}')
        ORDER BY duration DESC
        """,

        # 4) Gros GROUP BY sur users + sessions sans LIMIT
        """
        SELECT u.city, AVG(s.duration) AS avg_duration, COUNT(*) AS nb
        FROM users u
        JOIN sessions s ON u.id = s.user_id
        GROUP BY u.city
        ORDER BY avg_duration DESC
        """,

        # 5) Join large orders / order_items / products sans LIMIT
        """
        SELECT o.id AS order_id, u.city, p.category, SUM(oi.quantity * oi.unit_price) AS total
        FROM orders o
        JOIN users u       ON o.user_id = u.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p     ON p.id = oi.product_id
        GROUP BY o.id, u.city, p.category
        ORDER BY total DESC
        """,

        # 6) Sous-requête NOT IN (souvent lente si beaucoup de lignes)
        """
        SELECT *
        FROM users
        WHERE id NOT IN (
            SELECT DISTINCT user_id FROM sessions
        )
        """,

        # 7) Join + condition non indexée + pas de LIMIT
        """
        SELECT u.id, u.name, u.city, s.duration, s.session_type
        FROM users u
        JOIN sessions s ON u.id = s.user_id
        WHERE s.duration > {duration_min}
        ORDER BY s.duration DESC
        """,

        # 8) FULL SCAN sur products + ORDER BY non indexé (name)
        """
        SELECT *
        FROM products
        ORDER BY name DESC
        """
    ]


# ==========================
# 3) REMPLISSAGE DES TEMPLATES
# ==========================

def fill_fast_query(q: str) -> str:
    """Remplace les {placeholders} par des valeurs plausibles (FAST)."""
    city = random.choice(CITIES)
    age_min = random.randint(18, 40)
    age_max = age_min + random.randint(5, 20)
    user_id = random.randint(1, 4000)
    category = random.choice(["cardio", "muscu", "accessoire", "nutrition"])

    return q.format(
        city=city,
        age_min=age_min,
        age_max=age_max,
        user_id=user_id,
        category=category,
    )


def fill_slow_template(q: str) -> str:
    """Paramétrage de requêtes lentes (templates)."""
    city = random.choice(CITIES)
    substr = random.choice(["an", "ou", "el", "ar", "li"])
    date_str = "2025-01-01"
    duration_min = random.randint(1000, 5000)

    return q.format(
        city=city,
        substr=substr,
        date_str=date_str,
        duration_min=duration_min,
    )


# ==========================
# 4) TES 15 GROSSES REQUÊTES (GEN)
# ==========================

def q1_full_scan_sessions():
    return """
        SELECT *
        FROM sessions
        ORDER BY created_at DESC
        LIMIT 5000
    """


def q2_like_on_users():
    pattern_city = "%" + random.choice(["ar", "ou", "ll", "is"]) + "%"
    pattern_name = "%" + random.choice(["a", "e", "ou", "an"]) + "%"
    return f"""
        SELECT *
        FROM users
        WHERE city LIKE '{pattern_city}'
           OR name LIKE '{pattern_name}'
        ORDER BY created_at DESC
        LIMIT 1000
    """


def q3_age_range_users():
    a = random.randint(18, 50)
    b = a + random.randint(5, 20)
    return f"""
        SELECT id, name, age, city
        FROM users
        WHERE age BETWEEN {a} AND {b}
        ORDER BY age ASC
        LIMIT 2000
    """


def q4_sessions_last_days_with_join():
    days = random.randint(1, 60)
    min_sessions = random.randint(5, 50)
    city = random.choice(CITIES)
    return f"""
        SELECT u.id AS user_id, u.city, COUNT(s.id) AS nb_sessions
        FROM users u
        JOIN sessions s ON s.user_id = u.id
        WHERE s.created_at >= NOW() - INTERVAL {days} DAY
          AND u.city = '{city}'
        GROUP BY u.id, u.city
        HAVING nb_sessions > {min_sessions}
        ORDER BY nb_sessions DESC
        LIMIT 500
    """


def q5_avg_duration_by_city():
    return """
        SELECT u.city, AVG(s.duration) AS avg_duration
        FROM users u
        JOIN sessions s ON s.user_id = u.id
        GROUP BY u.city
        ORDER BY avg_duration DESC
        LIMIT 50
    """


def q6_sessions_by_type_device():
    return """
        SELECT session_type, device, COUNT(*) AS nb
        FROM sessions
        GROUP BY session_type, device
        ORDER BY nb DESC
    """


def q7_revenue_by_category_last_days():
    days = random.randint(7, 90)
    return f"""
        SELECT
            p.category,
            SUM(oi.quantity * oi.unit_price) AS revenue
        FROM order_items oi
        JOIN products p ON p.id = oi.product_id
        JOIN orders o ON o.id = oi.order_id
        WHERE o.created_at >= NOW() - INTERVAL {days} DAY
        GROUP BY p.category
        ORDER BY revenue DESC
    """


def q8_top_clients_paid_orders():
    threshold = random.randint(200, 3000)
    return f"""
        SELECT
            u.id AS user_id,
            u.city,
            SUM(o.total_amount) AS total_spent,
            COUNT(o.id) AS nb_orders
        FROM users u
        JOIN orders o ON o.user_id = u.id
        WHERE o.status = 'PAID'
        GROUP BY u.id, u.city
        HAVING total_spent > {threshold}
        ORDER BY total_spent DESC
        LIMIT 200
    """


def q9_correlated_subquery_users_big_spenders():
    threshold = random.randint(500, 5000)
    return f"""
        SELECT *
        FROM users u
        WHERE (
            SELECT SUM(o.total_amount)
            FROM orders o
            WHERE o.user_id = u.id
              AND o.status = 'PAID'
        ) > {threshold}
        ORDER BY u.created_at DESC
        LIMIT 500
    """


def q10_products_most_sold_by_category():
    qty_min = random.randint(5, 50)
    return f"""
        SELECT
            p.category,
            p.id AS product_id,
            p.name,
            SUM(oi.quantity) AS qty_sold,
            SUM(oi.quantity * oi.unit_price) AS revenue
        FROM order_items oi
        JOIN products p ON p.id = oi.product_id
        GROUP BY p.category, p.id, p.name
        HAVING qty_sold > {qty_min}
        ORDER BY revenue DESC
        LIMIT 100
    """


def q11_orders_recent_for_city():
    city = random.choice(CITIES)
    return f"""
        SELECT
            o.id AS order_id,
            o.status,
            o.total_amount,
            o.created_at,
            u.city
        FROM orders o
        JOIN users u ON u.id = o.user_id
        WHERE u.city = '{city}'
        ORDER BY o.created_at DESC
        LIMIT 500
    """


def q12_heavy_in_subquery():
    min_duration = random.randint(30, 120)
    days = random.randint(1, 30)
    return f"""
        SELECT *
        FROM users u
        WHERE u.id IN (
            SELECT DISTINCT s.user_id
            FROM sessions s
            WHERE s.duration > {min_duration}
              AND s.created_at >= NOW() - INTERVAL {days} DAY
        )
        ORDER BY u.created_at DESC
        LIMIT 1000
    """


def q13_nested_orders_items_products():
    min_items = random.randint(2, 10)
    days = random.randint(7, 90)
    return f"""
        SELECT
            u.id AS user_id,
            u.city,
            COUNT(DISTINCT o.id) AS nb_orders,
            SUM(oi.quantity) AS total_items,
            SUM(oi.quantity * oi.unit_price) AS total_spent
        FROM users u
        JOIN orders o ON o.user_id = u.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p ON p.id = oi.product_id
        WHERE o.created_at >= NOW() - INTERVAL {days} DAY
        GROUP BY u.id, u.city
        HAVING total_items > {min_items}
        ORDER BY total_spent DESC
        LIMIT 300
    """


def q14_like_and_in_combo():
    city_pattern = "%" + random.choice(["ar", "ou", "il", "is"]) + "%"
    min_duration = random.randint(20, 100)
    return f"""
        SELECT u.*
        FROM users u
        WHERE u.city LIKE '{city_pattern}'
          AND u.id IN (
              SELECT s.user_id
              FROM sessions s
              WHERE s.duration > {min_duration}
          )
        ORDER BY u.created_at DESC
        LIMIT 800
    """


def q15_orders_status_monthly():
    status = random.choice(ORDER_STATUSES)
    return f"""
        SELECT
            DATE_FORMAT(created_at, '%Y-%m') AS month,
            COUNT(*) AS nb_orders,
            SUM(total_amount) AS total_revenue
        FROM orders
        WHERE status = '{status}'
        GROUP BY month
        ORDER BY month DESC
        LIMIT 24
    """

# ==========================
# 2.bis) GROSSES REQUÊTES TRÈS LENTES (3–5 JOINS, IN, EXISTS, LIKE, etc.)
# ==========================

def q16_users_with_sessions_but_no_orders():
    # Users qui ont des sessions longues mais aucune commande
    min_duration = random.randint(30, 120)
    days = random.randint(7, 60)
    return f"""
        SELECT u.*
        FROM users u
        WHERE EXISTS (
            SELECT 1
            FROM sessions s
            WHERE s.user_id = u.id
              AND s.duration > {min_duration}
              AND s.created_at >= NOW() - INTERVAL {days} DAY
        )
          AND NOT EXISTS (
            SELECT 1
            FROM orders o
            WHERE o.user_id = u.id
          )
        ORDER BY u.created_at DESC
        LIMIT 1000
    """


def q17_big_join_users_orders_items_products():
    # JOIN sur 4 tables, group by large, HAVING, ORDER BY – bien lourd
    min_items = random.randint(3, 15)
    days = random.randint(15, 180)
    return f"""
        SELECT
            u.id              AS user_id,
            u.city            AS city,
            COUNT(DISTINCT o.id) AS nb_orders,
            SUM(oi.quantity)      AS total_items,
            SUM(oi.quantity * oi.unit_price) AS total_spent
        FROM users u
        JOIN orders o       ON o.user_id = u.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p     ON p.id = oi.product_id
        WHERE o.created_at >= NOW() - INTERVAL {days} DAY
        GROUP BY u.id, u.city
        HAVING total_items > {min_items}
        ORDER BY total_spent DESC
        LIMIT 500
    """


def q18_nested_subquery_with_aggregates():
    # Sous-requêtes imbriquées avec agrégations
    min_revenue = random.randint(500, 5000)
    return f"""
        SELECT *
        FROM (
            SELECT
                u.id AS user_id,
                u.city,
                (
                    SELECT SUM(o2.total_amount)
                    FROM orders o2
                    WHERE o2.user_id = u.id
                      AND o2.status = 'PAID'
                ) AS paid_revenue
            FROM users u
        ) AS t
        WHERE t.paid_revenue IS NOT NULL
          AND t.paid_revenue > {min_revenue}
        ORDER BY t.paid_revenue DESC
        LIMIT 300
    """


def q19_orders_with_max_city_spender():
    # Trouver, par ville, l'utilisateur qui dépense le plus (double niveau de group by)
    return """
        SELECT
            t.city,
            t.user_id,
            t.total_spent
        FROM (
            SELECT
                u.city,
                u.id AS user_id,
                SUM(o.total_amount) AS total_spent
            FROM users u
            JOIN orders o ON o.user_id = u.id
            GROUP BY u.city, u.id
        ) AS t
        JOIN (
            SELECT
                city,
                MAX(total_spent) AS max_spent
            FROM (
                SELECT
                    u.city,
                    u.id,
                    SUM(o.total_amount) AS total_spent
                FROM users u
                JOIN orders o ON o.user_id = u.id
                GROUP BY u.city, u.id
            ) AS inner_t
            GROUP BY city
        ) AS m
          ON t.city = m.city AND t.total_spent = m.max_spent
        ORDER BY t.total_spent DESC
    """


def q20_complex_like_and_in_on_users():
    # LIKE + IN (SELECT) mélangé, sans index sur les fonctions/LIKE
    pattern1 = "%" + random.choice(["an", "ou", "el", "ar", "li"]) + "%"
    pattern2 = "%" + random.choice(["user", "mail", "test"]) + "%"
    min_total = random.randint(100, 2000)
    return f"""
        SELECT u.*
        FROM users u
        WHERE (u.name LIKE '{pattern1}' OR u.email LIKE '{pattern2}')
          AND u.id IN (
              SELECT o.user_id
              FROM orders o
              GROUP BY o.user_id
              HAVING SUM(o.total_amount) > {min_total}
          )
        ORDER BY u.created_at DESC
        LIMIT 800
    """


def q21_heavy_orders_products_with_subselect():
    # join 3 tables + sous-select dans SELECT
    days = random.randint(30, 365)
    return f"""
        SELECT
            o.id AS order_id,
            o.created_at,
            (
                SELECT COUNT(*)
                FROM sessions s
                WHERE s.user_id = o.user_id
                  AND s.created_at >= o.created_at - INTERVAL {days} DAY
            ) AS nb_sessions_recent,
            p.category,
            SUM(oi.quantity * oi.unit_price) AS total_line
        FROM orders o
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p     ON p.id = oi.product_id
        WHERE o.created_at >= NOW() - INTERVAL {days} DAY
        GROUP BY o.id, o.created_at, p.category
        ORDER BY nb_sessions_recent DESC, total_line DESC
        LIMIT 500
    """


def q22_multi_join_with_not_exists():
    # 4 tables + NOT EXISTS
    min_qty = random.randint(2, 10)
    return f"""
        SELECT
            u.id AS user_id,
            u.city,
            COUNT(DISTINCT o.id) AS nb_orders,
            SUM(oi.quantity) AS total_qty
        FROM users u
        JOIN orders o       ON o.user_id = u.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p     ON p.id = oi.product_id
        WHERE p.category = 'Electronics'
          AND NOT EXISTS (
              SELECT 1
              FROM sessions s
              WHERE s.user_id = u.id
                AND s.session_type = 'web'
          )
        GROUP BY u.id, u.city
        HAVING total_qty > {min_qty}
        ORDER BY total_qty DESC
        LIMIT 300
    """


def q23_deep_nested_in_subquery():
    # IN (SELECT) imbriqué sur plusieurs niveaux
    return """
        SELECT *
        FROM users u
        WHERE u.id IN (
            SELECT o.user_id
            FROM orders o
            WHERE o.id IN (
                SELECT oi.order_id
                FROM order_items oi
                WHERE oi.product_id IN (
                    SELECT p.id
                    FROM products p
                    WHERE p.price > 100
                )
            )
        )
        ORDER BY u.created_at DESC
        LIMIT 1000
    """


def q24_5_tables_big_join():
    # 5 tables dans un gros join (users, orders, order_items, products, sessions)
    days = random.randint(7, 90)
    min_duration = random.randint(30, 120)
    return f"""
        SELECT
            u.id AS user_id,
            u.city,
            COUNT(DISTINCT o.id) AS nb_orders,
            SUM(oi.quantity * oi.unit_price) AS revenue,
            AVG(s.duration) AS avg_session_duration
        FROM users u
        JOIN orders o       ON o.user_id = u.id
        JOIN order_items oi ON oi.order_id = o.id
        JOIN products p     ON p.id = oi.product_id
        JOIN sessions s     ON s.user_id = u.id
        WHERE s.duration > {min_duration}
          AND s.created_at >= NOW() - INTERVAL {days} DAY
        GROUP BY u.id, u.city
        ORDER BY revenue DESC, avg_session_duration DESC
        LIMIT 300
    """


def q25_nested_exists_and_not_in():
    # EXISTS + NOT IN + LIKE
    city_pattern = "%" + random.choice(["ar", "ou", "il", "is"]) + "%"
    return f"""
        SELECT u.*
        FROM users u
        WHERE u.city LIKE '{city_pattern}'
          AND EXISTS (
              SELECT 1
              FROM orders o
              WHERE o.user_id = u.id
                AND o.status = 'PAID'
          )
          AND u.id NOT IN (
              SELECT s.user_id
              FROM sessions s
              WHERE s.session_type = 'api'
          )
        ORDER BY u.created_at DESC
        LIMIT 700
    """

def structural_variants():
    variants = []

    variants += [
        "SELECT id FROM users",
        "SELECT DISTINCT city FROM users",
        "SELECT COUNT(*) FROM users",
        "SELECT city, COUNT(*) FROM users GROUP BY city",
        "SELECT city FROM users ORDER BY city",
        "SELECT city FROM users ORDER BY city DESC",
        "SELECT city FROM users GROUP BY city HAVING COUNT(*) > 10",
        "SELECT * FROM users ORDER BY created_at",
        "SELECT * FROM users ORDER BY age",
        "SELECT * FROM users WHERE age > 30 ORDER BY age",
    ]

    variants += [
        "SELECT user_id, AVG(duration) FROM sessions GROUP BY user_id",
        "SELECT user_id FROM sessions GROUP BY user_id HAVING AVG(duration) > 60",
        "SELECT * FROM sessions ORDER BY duration",
        "SELECT * FROM sessions ORDER BY created_at",
        "SELECT session_type, COUNT(*) FROM sessions GROUP BY session_type",
        "SELECT device, COUNT(*) FROM sessions GROUP BY device",
    ]

    variants += [
        "SELECT u.city, COUNT(*) FROM users u JOIN sessions s ON u.id=s.user_id GROUP BY u.city",
        "SELECT u.city, AVG(s.duration) FROM users u JOIN sessions s ON u.id=s.user_id GROUP BY u.city",
        "SELECT u.id FROM users u WHERE EXISTS (SELECT 1 FROM sessions s WHERE s.user_id=u.id)",
        "SELECT * FROM users u WHERE u.id IN (SELECT user_id FROM sessions)",
    ]

    variants += [
        "SELECT * FROM orders ORDER BY total_amount",
        "SELECT status, COUNT(*) FROM orders GROUP BY status",
        "SELECT user_id, SUM(total_amount) FROM orders GROUP BY user_id",
        "SELECT * FROM orders WHERE total_amount > (SELECT AVG(total_amount) FROM orders)",
    ]

    variants += [
        "SELECT p.category, SUM(oi.quantity) FROM products p JOIN order_items oi ON p.id=oi.product_id GROUP BY p.category",
        "SELECT p.id FROM products p WHERE p.id NOT IN (SELECT product_id FROM order_items)",
    ]

    return variants

# ==========================
# 5) WORKLOAD PRINCIPAL FUSIONNÉ
# ==========================

def run_workload(n_queries: int = 150000, slow_ratio: float = 0.7):
    """
    Exécute un mélange de :
      - requêtes FAST (bien indexées, avec LIMIT)
      - requêtes SLOW_TEMPLATE (anti-patterns sur templates)
      - requêtes SLOW_GEN (q1..q15 plus lourdes)
      - requêtes SLOW_STRUCT (structural_variants, pour générer plein de DIGEST_TEXT différents)

    slow_ratio = proportion globale de requêtes lentes (templates + gen + struct).
    """

    conn = get_connection()
    cursor = conn.cursor()

    # Templates rapides / lents
    fast_templates = get_fast_queries()
    slow_templates = get_slow_templates()

    # Générateurs "lourds"
    slow_generators = [
        q1_full_scan_sessions,
        q2_like_on_users,
        q3_age_range_users,
        q4_sessions_last_days_with_join,
        q5_avg_duration_by_city,
        q6_sessions_by_type_device,
        q7_revenue_by_category_last_days,
        q8_top_clients_paid_orders,
        q9_correlated_subquery_users_big_spenders,
        q10_products_most_sold_by_category,
        q11_orders_recent_for_city,
        q12_heavy_in_subquery,
        q13_nested_orders_items_products,
        q14_like_and_in_combo,
        q15_orders_status_monthly,
        q16_users_with_sessions_but_no_orders,
        q17_big_join_users_orders_items_products,
        q18_nested_subquery_with_aggregates,
        q19_orders_with_max_city_spender,
        q20_complex_like_and_in_on_users,
        q21_heavy_orders_products_with_subselect,
        q22_multi_join_with_not_exists,
        q23_deep_nested_in_subquery,
        q24_5_tables_big_join,
        q25_nested_exists_and_not_in,
    ]

    # Variantes purement structurelles (pas de paramètres, beaucoup de DIGEST_TEXT différents)
    structural_sqls = structural_variants()

    n_fast = 0
    n_slow_template = 0
    n_slow_gen = 0
    n_slow_struct = 0

    print(f"[INFO] Running workload with n_queries={n_queries}, slow_ratio={slow_ratio}")

    for i in range(1, n_queries + 1):
        r = random.random()

        if r < slow_ratio:
            # Zone "lente"
            r_slow = random.random()

            if r_slow < 0.4:
                # 40% → template lente (anti-pattern)
                template = random.choice(slow_templates)
                sql = fill_slow_template(template)
                kind = "SLOW_TEMPLATE"
                n_slow_template += 1

            elif r_slow < 0.8:
                # 40% → gros générateurs q1..q15
                gen = random.choice(slow_generators)
                sql = gen()
                kind = "SLOW_GEN"
                n_slow_gen += 1

            else:
                # 20% → requêtes structurelles
                sql = random.choice(structural_sqls)
                kind = "SLOW_STRUCT"
                n_slow_struct += 1

        else:
            # ⚡️ Zone "rapide"
            r_fast = random.random()

            if r_fast < 0.8:
                # 80% → template rapide optimisée
                template = random.choice(fast_templates)
                sql = fill_fast_query(template)
                kind = "FAST"
                n_fast += 1
            else:
                # 20% → structurelle (mais souvent pas trop lente)
                sql = random.choice(structural_sqls)
                kind = "SLOW_STRUCT"
                n_slow_struct += 1

        try:
            cursor.execute(sql)
            cursor.fetchall()
        except mysql.connector.Error as e:
            print(f"[WARN] Query ({kind}) failed: {str(e)[:120]}")

        if i % 1000 == 0:
            print(
                f"[INFO] {i}/{n_queries} queries executed "
                f"(FAST={n_fast}, SLOW_TEMPLATE={n_slow_template}, "
                f"SLOW_GEN={n_slow_gen}, SLOW_STRUCT={n_slow_struct})"
            )

    cursor.close()
    conn.close()
    print(
        f"[INFO] Workload finished. "
        f"FAST={n_fast}, SLOW_TEMPLATE={n_slow_template}, "
        f"SLOW_GEN={n_slow_gen}, SLOW_STRUCT={n_slow_struct}"
    )


if __name__ == "__main__":
    run_workload(n_queries=150000, slow_ratio=0.7)