#!/bin/bash
set -euo pipefail

echo ">>> docker-init.sh starting"

# Ждём Postgres
echo "Waiting for Postgres (db:5432)..."
for i in $(seq 1 60); do
  if pg_isready -h db -p 5432 -U superset_user -d superset_db >/dev/null 2>&1; then
    echo "Postgres is ready"
    break
  fi
  printf '.'
  sleep 1
done
echo ""

INIT_FLAG="/app/superset_home/.superset_initialized"
mkdir -p /app/superset_home

if [ ! -f "$INIT_FLAG" ]; then
  echo ">>> Initializing Superset DB and creating admin"

  # Выполняем миграции
  superset db upgrade

  # Создаём admin-пользователя (изменяй пароль при желании)
  superset fab create-admin \
    --username Ali \
    --firstname Alikhan \
    --lastname Kassymbekov \
    --email kassali2005@gmail.com \
    --password Alikhan2311


  # Инициализация ролей/разрешений
  superset init

  touch "$INIT_FLAG"
  echo ">>> Superset initialized"
else
  echo ">>> Superset already initialized, skipping"
fi

echo ">>> Starting Superset webserver"
exec superset run -p 8088 --with-threads --host 0.0.0.0
