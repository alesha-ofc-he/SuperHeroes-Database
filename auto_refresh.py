#!/usr/bin/env python3
"""
DB Activity Simulator для Prometheus/Grafana метрик
Постоянно выполняет SELECT/INSERT/UPDATE запросы для создания активности
"""
import os
import time
import random
import psycopg2
from datetime import datetime

# DB Connection
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5433")
DB_NAME = os.getenv("DB_NAME", "superset_db")
DB_USER = os.getenv("DB_USER", "superset_user")
DB_PASS = os.getenv("DB_PASS", "Pg!Super1234")

def get_connection():
    return psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        database=DB_NAME,
        user=DB_USER,
        password=DB_PASS
    )

def simulate_activity():
    """Постоянно выполняет разные типы запросов"""
    try:
        conn = get_connection()
        cur = conn.cursor()
        
        # 1. SELECT запросы
        cur.execute("SELECT COUNT(*) FROM superhero.superhero;")
        hero_count = cur.fetchone()[0]
        print(f"[{datetime.now()}] Total heroes: {hero_count}")
        
        # 2. Проверка среднего значения атрибута
        cur.execute("""
            SELECT AVG(attribute_value) 
            FROM superhero.hero_attribute 
            WHERE attribute_id = 1;
        """)
        avg_intelligence = cur.fetchone()[0]
        print(f"[{datetime.now()}] Avg Intelligence: {avg_intelligence}")
        
        # 3. CREATE временная таблица и вставка (симуляция активности)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS superhero.activity_log (
                id SERIAL PRIMARY KEY,
                log_time TIMESTAMP DEFAULT NOW(),
                activity_type VARCHAR(50),
                value NUMERIC
            );
        """)
        
        # 4. INSERT новая запись
        activity_type = random.choice(['select', 'update', 'insert'])
        value = random.uniform(50, 100)
        cur.execute("""
            INSERT INTO superhero.activity_log (activity_type, value)
            VALUES (%s, %s);
        """, (activity_type, value))
        
        conn.commit()
        print(f"[{datetime.now()}] Inserted activity log: {activity_type}, {value}")
        
        cur.close()
        conn.close()
        
    except Exception as e:
        print(f"[{datetime.now()}] Error: {e}")

if __name__ == "__main__":
    print("🚀 Starting DB Activity Simulator...")
    print(f"Connecting to {DB_HOST}:{DB_PORT}/{DB_NAME}")
    
    while True:
        simulate_activity()
        time.sleep(5)  # Каждые 5 секунд
