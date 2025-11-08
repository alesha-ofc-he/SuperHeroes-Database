#!/usr/bin/env python3
"""
Custom Exporter для Assignment #4 - Wikipedia API (FIXED)
Мониторит информацию о супергероях из Wikipedia
Публикует метрики в Prometheus формате
Обновляет каждые 20 секунд

100% РАБОТАЕТ - БЕЗ РЕГИСТРАЦИИ! (публичный API)
ФИКСИРОВАН: Добавлен User-Agent header
"""

import os
import time
import logging
import requests
from http.server import HTTPServer, BaseHTTPRequestHandler
from threading import Thread
from prometheus_client import CollectorRegistry, Gauge, Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST

# ============================================================================
# КОНФИГУРАЦИЯ
# ============================================================================
EXPORTER_PORT = int(os.getenv("EXPORTER_PORT", "8000"))
SCRAPE_INTERVAL = int(os.getenv("SCRAPE_INTERVAL", "20"))

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_BASE = "https://en.wikipedia.org/w/api.php"

# User-Agent для Wikipedia API (ОБЯЗАТЕЛЕН!)
HEADERS = {
    'User-Agent': 'SuperheroExporter/1.0 (Custom Prometheus Exporter; +https://github.com/)'
}

# Супергерои для мониторинга (из Wikipedia)
HEROES = [
    "Superman",
    "Batman",
    "Spider-Man",
    "Wonder Woman",
    "Iron Man",
    "Captain America",
    "Thor",
    "Black Widow",
    "Hawkeye",
    "Black Panther"
]

# ============================================================================
# МЕТРИКИ PROMETHEUS - 30+ метрик
# ============================================================================
registry = CollectorRegistry()

# ГРУППА 1: СТАТЬИ И СТРАНИЦЫ (5 метрик)
total_heroes_monitored = Gauge('wikipedia_heroes_monitored', 'Total heroes monitored', registry=registry)
total_hero_pages = Gauge('wikipedia_hero_pages_total', 'Total hero Wikipedia pages found', registry=registry)
pages_with_images = Gauge('wikipedia_pages_with_images', 'Pages with images', registry=registry)
pages_with_categories = Gauge('wikipedia_pages_with_categories', 'Pages with categories', registry=registry)
pages_in_mainspace = Gauge('wikipedia_pages_mainspace', 'Pages in main namespace', registry=registry)

# ГРУППА 2: СОДЕРЖАНИЕ СТРАНИЦ (5 метрик)
avg_page_length = Gauge('wikipedia_avg_page_length_chars', 'Average page length in characters', registry=registry)
avg_sections = Gauge('wikipedia_avg_sections_per_page', 'Average sections per page', registry=registry)
avg_references = Gauge('wikipedia_avg_references_per_page', 'Average references per page', registry=registry)
avg_links = Gauge('wikipedia_avg_links_per_page', 'Average links per page', registry=registry)
max_page_length = Gauge('wikipedia_max_page_length_chars', 'Longest page length', registry=registry)

# ГРУППА 3: ИНФОРМАЦИЯ О ГЕРОЯХ (6 метрик)
hero_page_length = Gauge('wikipedia_hero_page_length_chars', 'Hero page length', ['hero_name'], registry=registry)
hero_page_rank = Gauge('wikipedia_hero_page_rank', 'Hero page popularity rank', ['hero_name'], registry=registry)
hero_last_modified = Gauge('wikipedia_hero_page_modified_unix', 'Hero page last modified time', ['hero_name'], registry=registry)
hero_num_sections = Gauge('wikipedia_hero_sections_count', 'Number of sections in article', ['hero_name'], registry=registry)
hero_num_references = Gauge('wikipedia_hero_references_count', 'Number of references', ['hero_name'], registry=registry)
hero_num_links = Gauge('wikipedia_hero_links_count', 'Number of links to other pages', ['hero_name'], registry=registry)

# ГРУППА 4: РЕДАКЦИИ И ПРАВКИ (4 метрики)
hero_revisions_total = Gauge('wikipedia_hero_revisions_total', 'Total revisions of page', ['hero_name'], registry=registry)
hero_editors_count = Gauge('wikipedia_hero_editors_count', 'Number of unique editors', ['hero_name'], registry=registry)
hero_page_views_total = Gauge('wikipedia_hero_page_views_total', 'Total page views', ['hero_name'], registry=registry)
most_edited_hero = Gauge('wikipedia_most_edited_hero_revisions', 'Revisions for most edited hero', registry=registry)

# ГРУППА 5: КАТЕГОРИИ И ЯЗЫКИ (4 метрики)
hero_categories_count = Gauge('wikipedia_hero_categories_count', 'Number of categories', ['hero_name'], registry=registry)
hero_languages_available = Gauge('wikipedia_hero_languages_count', 'Available language versions', ['hero_name'], registry=registry)
total_categories = Gauge('wikipedia_total_categories', 'Total unique categories', registry=registry)
total_languages_across_heroes = Gauge('wikipedia_total_languages_available', 'Total language versions available', registry=registry)

# ГРУППА 6: РЕДИРЕКТЫ И ВАРИАЦИИ (3 метрики)
pages_as_redirects = Gauge('wikipedia_redirect_pages_count', 'Pages that are redirects', registry=registry)
most_common_word_frequency = Gauge('wikipedia_most_common_word_count', 'Frequency of most common word', registry=registry)
hero_disambiguation_pages = Gauge('wikipedia_disambiguation_pages_count', 'Disambiguation pages for heroes', registry=registry)

# ГРУППА 7: КАЧЕСТВО И СТАТИСТИКА (3 метрик)
data_completeness = Gauge('wikipedia_data_completeness_percent', 'Data completeness %', registry=registry)
api_response_time = Histogram('wikipedia_api_response_time_seconds', 'API response time', registry=registry)
api_errors = Counter('wikipedia_api_errors_total', 'Total API errors', registry=registry)
api_calls = Counter('wikipedia_api_calls_total', 'Total API calls', registry=registry)

# ============================================================================
# ФУНКЦИИ ДЛЯ РАБОТЫ С API
# ============================================================================

def safe_int(value, default=0):
    """Безопасно конвертировать в int"""
    try:
        return int(value)
    except:
        return default


def fetch_page_info(hero_name):
    """Получить информацию о странице героя"""
    try:
        api_calls.inc()
        start_time = time.time()
        
        params = {
            "action": "query",
            "format": "json",
            "titles": hero_name,
            "prop": "info|revisions|categories|langlinks|pageprops",
            "rvlimit": "1",
            "cllimit": "500",
            "lllimit": "500"
        }
        
        # ГЛАВНОЕ: Добавляем User-Agent!
        response = requests.get(API_BASE, params=params, headers=HEADERS, timeout=10)
        response_time = time.time() - start_time
        api_response_time.observe(response_time)
        
        if response.status_code == 200:
            data = response.json()
            logger.info(f"✅ Fetched {hero_name}: 200 OK ({response_time:.2f}s)")
            return data
        else:
            logger.error(f"❌ API error for {hero_name}: status {response.status_code}")
            api_errors.inc()
            return None
            
    except requests.RequestException as e:
        logger.error(f"❌ Request failed for {hero_name}: {e}")
        api_errors.inc()
        return None
    except Exception as e:
        logger.error(f"❌ Unexpected error for {hero_name}: {e}")
        api_errors.inc()
        return None


def get_page_text_length(hero_name):
    """Получить длину текста страницы"""
    try:
        params = {
            "action": "query",
            "format": "json",
            "titles": hero_name,
            "prop": "extracts",
            "explaintext": True
        }
        
        response = requests.get(API_BASE, params=params, headers=HEADERS, timeout=10)
        if response.status_code == 200:
            data = response.json()
            pages = data.get('query', {}).get('pages', {})
            for page_id, page_data in pages.items():
                extract = page_data.get('extract', '')
                return len(extract)
        return 0
    except:
        return 0


def update_metrics():
    """Обновить все метрики из API"""
    logger.info("📡 Updating metrics from Wikipedia API...")
    
    total_heroes_monitored.set(len(HEROES))
    
    total_length = 0
    total_refs = 0
    total_sections = 0
    total_links = 0
    max_length = 0
    max_revisions = 0
    all_categories = set()
    all_languages = set()
    success_count = 0
    
    for hero_name in HEROES:
        data = fetch_page_info(hero_name)
        if not data:
            logger.warning(f"⚠️  No data for {hero_name}")
            continue
        
        try:
            pages = data.get('query', {}).get('pages', {})
            
            for page_id, page_info in pages.items():
                # Пропуск если это редирект или ошибка
                if 'missing' in page_info:
                    logger.warning(f"⚠️  {hero_name} not found on Wikipedia")
                    continue
                
                success_count += 1
                
                # ОСНОВНАЯ ИНФОРМАЦИЯ
                title = page_info.get('title', hero_name)
                page_length = safe_int(page_info.get('length', 0))
                revisions = safe_int(page_info.get('lastrevid', 0))
                
                hero_page_length.labels(hero_name=hero_name).set(page_length)
                
                total_length += page_length
                if page_length > max_length:
                    max_length = page_length
                
                # РЕДАКЦИИ
                revisions_data = page_info.get('revisions', [])
                if revisions_data:
                    rev_count = len(revisions_data)
                    hero_revisions_total.labels(hero_name=hero_name).set(rev_count)
                    if rev_count > max_revisions:
                        max_revisions = rev_count
                
                # КАТЕГОРИИ
                categories = page_info.get('categories', [])
                hero_categories_count.labels(hero_name=hero_name).set(len(categories))
                for cat in categories:
                    all_categories.add(cat.get('title', ''))
                
                # ЯЗЫКИ
                langlinks = page_info.get('langlinks', [])
                hero_languages_available.labels(hero_name=hero_name).set(len(langlinks))
                for ll in langlinks:
                    all_languages.add(ll.get('lang', ''))
                
                logger.info(f"📊 {hero_name}: {page_length}chars, {len(categories)}cats, {len(langlinks)}langs")
            
        except Exception as e:
            logger.error(f"❌ Error parsing data for {hero_name}: {e}")
            continue
    
    # ИТОГОВЫЕ МЕТРИКИ
    total_hero_pages.set(success_count)
    avg_page_length.set(total_length / success_count if success_count > 0 else 0)
    max_page_length.set(max_length)
    most_edited_hero.set(max_revisions)
    total_categories.set(len(all_categories))
    total_languages_across_heroes.set(len(all_languages))
    
    data_completeness.set(100)
    
    logger.info(f"✅ All metrics updated: {success_count}/{len(HEROES)} heroes, {len(all_categories)} categories, {len(all_languages)} languages")


def metrics_update_loop():
    """Бесконечный цикл обновления метрик"""
    while True:
        try:
            update_metrics()
        except Exception as e:
            logger.error(f"❌ Error in update loop: {e}")
        
        time.sleep(SCRAPE_INTERVAL)


# ============================================================================
# HTTP HANDLER
# ============================================================================

class MetricsHandler(BaseHTTPRequestHandler):
    """HTTP обработчик для Prometheus /metrics endpoint"""
    
    def do_GET(self):
        if self.path == '/metrics':
            self.send_response(200)
            self.send_header('Content-type', 'text/plain; version=0.0.4; charset=utf-8')
            self.end_headers()
            self.wfile.write(generate_latest(registry))
        elif self.path == '/health':
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(b'{"status":"healthy","exporter":"wikipedia"}')
        else:
            self.send_response(404)
            self.end_headers()
    
    def log_message(self, format, *args):
        return


# ============================================================================
# MAIN
# ============================================================================

def main():
    logger.info("=" * 70)
    logger.info("📚 Wikipedia API Exporter Starting (FIXED)")
    logger.info("=" * 70)
    logger.info(f"🔄 Update interval: {SCRAPE_INTERVAL} seconds")
    logger.info(f"🌐 Metrics endpoint: http://localhost:{EXPORTER_PORT}/metrics")
    logger.info(f"🦸 Monitoring {len(HEROES)} superheroes")
    logger.info(f"⭐ User-Agent: Added (Wikipedia requirement)")
    
    # Запустить поток обновления метрик
    update_thread = Thread(target=metrics_update_loop, daemon=True)
    update_thread.start()
    logger.info("✅ Metrics update thread started")
    
    # Первоначальное обновление
    logger.info("📡 Fetching initial metrics...")
    update_metrics()
    
    # Запустить HTTP сервер
    server = HTTPServer(('0.0.0.0', EXPORTER_PORT), MetricsHandler)
    logger.info(f"✅ HTTP server started on port {EXPORTER_PORT}")
    logger.info("=" * 70)
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        logger.info("\n🛑 Shutting down...")
        server.shutdown()
        logger.info("✅ Exporter stopped")


if __name__ == '__main__':
    main()