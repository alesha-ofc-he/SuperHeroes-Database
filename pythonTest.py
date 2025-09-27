import pandas as pd
import psycopg2

# Настройки подключения
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'superheroes',
    'user': 'postgres',
    'password': '1234'
}

# Ваши запросы (указаны таблицы со схемой superhero)
QUERIES = {
    'sample_heroes': """
        SELECT id, superhero_name, full_name, publisher_id
        FROM superhero.superhero
        LIMIT 10;
    """,
    'top_by_attributes': """
        SELECT s.id, s.superhero_name, COALESCE(SUM(ha.attribute_value), 0) AS total_attributes
        FROM superhero.superhero s
        LEFT JOIN superhero.hero_attribute ha ON s.id = ha.hero_id
        GROUP BY s.id, s.superhero_name
        ORDER BY total_attributes DESC
        LIMIT 20;
    """,
    'powers_count_by_publisher': """
        SELECT sp.power_name, COUNT(DISTINCT hp.hero_id) AS heroes_with_power
        FROM superhero.superpower sp
        JOIN superhero.hero_power hp ON sp.id = hp.power_id
        JOIN superhero.superhero s ON hp.hero_id = s.id
        GROUP BY sp.power_name
        ORDER BY heroes_with_power DESC
        LIMIT 50;
    """
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def run_query(conn, sql):
    return pd.read_sql(sql, conn)

def main():
    conn = get_connection()
    try:
        # Устанавливаем search_path в схему superhero, чтобы можно было без схемы
        with conn.cursor() as cur:
            cur.execute("SET search_path TO superhero;")
        for name, q in QUERIES.items():
            print(f"\n--- Running query: {name} ---")
            df = run_query(conn, q)
            print(df.to_string(index=False))
    finally:
        conn.close()

if __name__ == "__main__":
    main()
