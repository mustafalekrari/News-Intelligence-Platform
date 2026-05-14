"""
main.py — Orchestrateur principal du scraper
Lance tous les scrapers toutes les heures (batch) + Kafka (streaming)
"""
import logging
import os
import time
import schedule
from datetime import datetime

from hespress import scrape_hespress
from akhbarona import scrape_akhbarona
from rss_scraper import scrape_rss_sources
from bbc import scrape_bbc
from minio_storage import save_articles_to_bronze
from kafka_producer import get_producer, send_articles

# ── Logging ────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("main")

INTERVAL = int(os.getenv("SCRAPING_INTERVAL_MINUTES", "60"))


def run_scraping_cycle():
    """Lance un cycle complet de scraping sur toutes les sources."""
    logger.info("=" * 60)
    logger.info(f"CYCLE DE SCRAPING DÉMARRÉ — {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)

    all_articles = []

    # ── 1. Hespress (Maroc) ────────────────────────────────
    try:
        logger.info("→ Scraping Hespress...")
        articles = scrape_hespress()
        all_articles.extend(articles)
    except Exception as e:
        logger.error(f"Hespress ÉCHEC : {e}")

    # ── 2. Akhbarona (Maroc) ───────────────────────────────
    try:
        logger.info("→ Scraping Akhbarona...")
        articles = scrape_akhbarona()
        all_articles.extend(articles)
    except Exception as e:
        logger.error(f"Akhbarona ÉCHEC : {e}")

    # ── 3. BBC News ────────────────────────────────────────
    try:
        logger.info("→ Scraping BBC News...")
        articles = scrape_bbc()
        all_articles.extend(articles)
    except Exception as e:
        logger.error(f"BBC ÉCHEC : {e}")

    # ── 4. Al Jazeera + Reuters + CNN (RSS) ───────────────
    try:
        logger.info("→ Scraping RSS (Al Jazeera, Reuters, CNN)...")
        articles = scrape_rss_sources()
        all_articles.extend(articles)
    except Exception as e:
        logger.error(f"RSS ÉCHEC : {e}")

    logger.info(f"Total articles collectés : {len(all_articles)}")

    if not all_articles:
        logger.warning("Aucun article collecté dans ce cycle")
        return

    # ── 4. Déduplication par url_hash ─────────────────────
    seen = set()
    unique_articles = []
    for art in all_articles:
        h = art.get("url_hash", "")
        if h and h not in seen:
            seen.add(h)
            unique_articles.append(art)

    logger.info(f"Après déduplication : {len(unique_articles)} articles uniques")

    # ── 5. Sauvegarde Bronze (MinIO) ──────────────────────
    try:
        saved = save_articles_to_bronze(unique_articles)
        logger.info(f"MinIO Bronze : {saved} articles sauvegardés")
    except Exception as e:
        logger.error(f"MinIO ÉCHEC : {e}")

    # ── 6. Envoi Kafka (Streaming) ────────────────────────
    try:
        producer = get_producer()
        if producer:
            sent = send_articles(producer, unique_articles)
            producer.close()
            logger.info(f"Kafka : {sent} événements publiés")
        else:
            logger.warning("Kafka non disponible — streaming ignoré")
    except Exception as e:
        logger.error(f"Kafka ÉCHEC : {e}")

    logger.info(f"CYCLE TERMINÉ — {datetime.utcnow().isoformat()}")
    logger.info("=" * 60)


def main():
    logger.info("🚀 News Scraper Platform démarré")
    logger.info(f"Intervalle de scraping : {INTERVAL} minutes")

    # Premier cycle immédiat au démarrage
    run_scraping_cycle()

    # Planification des cycles suivants
    schedule.every(INTERVAL).minutes.do(run_scraping_cycle)

    logger.info(f"Prochain cycle dans {INTERVAL} minutes...")

    while True:
        schedule.run_pending()
        time.sleep(30)


if __name__ == "__main__":
    main()
