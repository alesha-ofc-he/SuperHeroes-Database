#!/usr/bin/env python3
"""
Database Load Generator
Постоянно создает нагрузку на БД для обновления метрик в Prometheus/Grafana
"""
import psycopg2
import time
import random
from datetime import datetime

# DB Connection Parameters
DB_CONFIG = {
    "host": "localhost",
    "port": 5433,
    "database": "superset_db",
    "user": "superset_user",
    "password": "Pg!Super1234"
}

def get_db_connection():
    """Подключение к БД"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        return conn
    except Exception as e:
        print(f"❌ Connection error: {e}")
        return None

def run_load_generator():
    """Основной генератор нагрузки"""
    print("🚀 Starting Database Load Generator...")
    print(f"📊 Target: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    
    iteration = 0
    
    while True:
        try:
            conn = get_db_connection()
            if not conn:
                time.sleep(5)
                continue
            
            cur = conn.cursor()
            iteration += 1
            
            # ===== 1. SELECT запросы =====
            cur.execute("SELECT COUNT(*) FROM superhero.superhero;")
            hero_count = cur.fetchone()[0]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ SELECT: Found {hero_count} heroes")
            
            # ===== 2. Сложный SELECT (для рейта) =====
            cur.execute("""
                SELECT AVG(attribute_value) 
                FROM superhero.hero_attribute 
                WHERE attribute_id = 1;
            """)
            avg_val = cur.fetchone()[0]
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ AVG Query: {avg_val:.2f}")
            
            # ===== 3. CREATE таблица для логирования (если нет) =====
            cur.execute("""
                CREATE TABLE IF NOT EXISTS superhero.activity_log (
                    id SERIAL PRIMARY KEY,
                    activity_type VARCHAR(50),
                    hero_id INT,
                    value NUMERIC,
                    created_at TIMESTAMP DEFAULT NOW()
                );
            """)
            
            # ===== 4. INSERT новые логи (для увеличения коммитов) =====
            for _ in range(5):
                activity = random.choice(['select', 'insert', 'update', 'delete'])
                hero_id = random.randint(1, min(50, hero_count)) if hero_count > 0 else 1
                value = random.uniform(10, 100)
                
                cur.execute("""
                    INSERT INTO superhero.activity_log (activity_type, hero_id, value)
                    VALUES (%s, %s, %s);
                """, (activity, hero_id, value))
            
            conn.commit()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ INSERT: 5 activity logs")
            
            # ===== 5. UPDATE операции =====
            cur.execute("""
                UPDATE superhero.activity_log 
                SET value = value + 1 
                WHERE created_at > NOW() - INTERVAL '1 minute'
                LIMIT 10;
            """)
            
            conn.commit()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ UPDATE: activity logs")
            
            # ===== 6. DELETE старые логи =====
            cur.execute("""
                DELETE FROM superhero.activity_log 
                WHERE created_at < NOW() - INTERVAL '1 hour';
            """)
            
            conn.commit()
            print(f"[{datetime.now().strftime('%H:%M:%S')}] ✅ DELETE: old logs")
            
            # ===== 7. Агрегирующие запросы =====
            cur.execute("""
                SELECT 
                    activity_type,
                    COUNT(*) as cnt,
                    AVG(value) as avg_val,
                    MAX(value) as max_val
                FROM superhero.activity_log
                WHERE created_at > NOW() - INTERVAL '10 minutes'
                GROUP BY activity_type;
            """)
            
            results = cur.fetchall()
            for row in results:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] 📈 {row[0]}: {row[1]} count, avg={row[2]:.2f}")
            
            cur.close()
            conn.close()
            
            print(f"✨ Iteration #{iteration} complete\n")
            
            # Sleep перед следующим циклом
            time.sleep(3)
            
        except Exception as e:
            print(f"⚠️  Error: {e}")
            time.sleep(5)

if __name__ == "__main__":
    try:
        run_load_generator()
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user")
        exit(0)
