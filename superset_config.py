# superset_config.py
# Жёстко задаем SECRET_KEY и строку подключения к Postgres
SECRET_KEY = "Sup3r$ecretKey_9tW"  # внутри контейнера $ будет как есть (мы эскейпнули в docker-compose)

# Жёстко задаём строку подключения (не используем переменные окружения, чтобы избежать подстановки "Ali")
SQLALCHEMY_DATABASE_URI = "postgresql+psycopg2://superset_user:Pg!Super1234@db:5432/superset_db"

# Рекомендуемые опции (необязательно)
SQLALCHEMY_POOL_RECYCLE = 3600
