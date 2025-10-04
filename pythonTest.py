import pandas as pd
import psycopg2

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'dbname': 'superheroes',
    'user': 'postgres',
    'password': '1234'
}

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
    """,
    'alignment_count': """
        SELECT al.alignment, COUNT(s.id) AS cnt
        FROM superhero.alignment al
        LEFT JOIN superhero.superhero s ON s.alignment_id = al.id
        GROUP BY al.alignment
        ORDER BY cnt DESC;
    """,
    'tallest_heroes': """
        SELECT id, superhero_name, height_cm
        FROM superhero.superhero
        WHERE height_cm IS NOT NULL
        ORDER BY height_cm DESC
        LIMIT 10;
    """,
    'heaviest_heroes': """
        SELECT id, superhero_name, weight_kg
        FROM superhero.superhero
        WHERE weight_kg IS NOT NULL
        ORDER BY weight_kg DESC
        LIMIT 10;
    """,
    'heroes_no_attributes_or_powers': """
        SELECT s.id, s.superhero_name
        FROM superhero.superhero s
        LEFT JOIN superhero.hero_attribute ha ON s.id = ha.hero_id
        LEFT JOIN superhero.hero_power hp ON s.id = hp.hero_id
        WHERE ha.hero_id IS NULL AND hp.hero_id IS NULL
        LIMIT 50;
    """,
    'most_powers': """
        SELECT s.id, s.superhero_name, COUNT(hp.power_id) AS power_count
        FROM superhero.superhero s
        LEFT JOIN superhero.hero_power hp ON s.id = hp.hero_id
        GROUP BY s.id, s.superhero_name
        ORDER BY power_count DESC
        LIMIT 20;
    """,
    'average_attributes_by_alignment': """
        WITH totals AS (
          SELECT s.id, s.alignment_id, COALESCE(SUM(ha.attribute_value),0) AS total_attr
          FROM superhero.superhero s
          LEFT JOIN superhero.hero_attribute ha ON s.id = ha.hero_id
          GROUP BY s.id, s.alignment_id
        )
        SELECT al.alignment, AVG(t.total_attr)::numeric(8,2) AS avg_total_attributes
        FROM totals t
        LEFT JOIN superhero.alignment al ON t.alignment_id = al.id
        GROUP BY al.alignment
        ORDER BY avg_total_attributes DESC;
    """,
    'race_distribution': """
        SELECT r.race, COUNT(s.id) AS cnt
        FROM superhero.race r
        LEFT JOIN superhero.superhero s ON s.race_id = r.id
        GROUP BY r.race
        ORDER BY cnt DESC;
    """
}

def get_connection():
    return psycopg2.connect(**DB_CONFIG)

def run_query(conn, sql):
    return pd.read_sql(sql, conn)

def main():
    conn = get_connection()
    try:
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
