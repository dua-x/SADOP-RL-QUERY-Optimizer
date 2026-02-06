import time
import mysql.connector
from config import MYSQL_CONFIG


def get_connection(retries: int = 5, delay: float = 1.0):
    """
    Ouvre une connexion MySQL avec quelques retries
    (pour laisser le temps au conteneur mysql de démarrer).
    """
    last_error = None

    for attempt in range(1, retries + 1):
        try:
            print(f"[DEBUG] Trying MySQL connection with config: {MYSQL_CONFIG}")
            conn = mysql.connector.connect(**MYSQL_CONFIG)
            print("[DEBUG] MySQL connection OK")
            return conn
        except mysql.connector.Error as e:
            last_error = e
            print(f"[WARN] MySQL connection failed (try {attempt}/{retries}): {e}")
            time.sleep(delay)

    # Si on arrive ici → tout a échoué
    raise last_error