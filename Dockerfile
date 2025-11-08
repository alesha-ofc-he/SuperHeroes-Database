FROM python:3.11-slim

# Установка системных зависимостей, необходимых для psycopg2
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    gcc \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

# Установка Python-зависимостей
RUN pip install --no-cache-dir -r requirements.txt

# Копирование основного скрипта
COPY auto_refresh.py .

CMD ["python", "auto_refresh.py"]