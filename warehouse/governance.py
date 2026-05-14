"""
Gouvernance des données — Traçabilité et documentation du pipeline
Génère un catalogue de données et des rapports de lignage
"""
import json
import logging
import os
from datetime import datetime

import psycopg2
import psycopg2.extras

logger = logging.getLogger(__name__)

DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB   = os.getenv("DWH_DB", "news_dwh")
DWH_USER = os.getenv("DWH_USER", "news_user")
DWH_PASS = os.getenv("DWH_PASSWORD", "news_pass")

DATA_CATALOG = {
    "platform": "News Data Platform",
    "version": "1.0.0",
    "layers": {
        "bronze": {
            "description": "Données brutes collectées par les scrapers",
            "storage": "MinIO bucket: bronze",
            "format": "JSON",
            "retention": "90 jours",
            "fields": ["url_hash", "url", "title", "author", "content", "category",
                       "source", "country", "language", "published_at", "scraped_at"],
        },
        "silver": {
            "description": "Données nettoyées et normalisées",
            "storage": "MinIO bucket: silver",
            "format": "JSON",
            "transformations": ["suppression HTML", "normalisation texte", "détection langue",
                                "calcul word_count", "déduplication"],
            "fields": ["+ word_count", "+ content_preview", "+ silver_processed_at"],
        },
        "gold": {
            "description": "Données enrichies avec mots-clés TF-IDF, prêtes pour l'analyse",
            "storage": "MinIO bucket: gold + PostgreSQL DWH",
            "format": "JSON + SQL",
            "transformations": ["extraction mots-clés TF-IDF", "calcul quality_score",
                                "chargement DWH", "agrégations"],
            "tables": ["fact_articles", "fact_keywords", "dim_date", "dim_source",
                       "dim_category", "dim_language"],
        },
    },
    "sources": [
        {"name": "Hespress", "country": "MA", "language": "ar", "method": "HTML scraping"},
        {"name": "Akhbarona", "country": "MA", "language": "ar", "method": "HTML scraping"},
        {"name": "BBC News", "country": "GB", "language": "en", "method": "HTML scraping"},
        {"name": "Al Jazeera", "country": "QA", "language": "en", "method": "RSS feed"},
        {"name": "Reuters", "country": "GB", "language": "en", "method": "RSS feed"},
        {"name": "CNN", "country": "US", "language": "en", "method": "RSS feed"},
    ],
    "pipeline": {
        "orchestration": "Apache Airflow",
        "schedule": "Toutes les heures (0 * * * *)",
        "steps": [
            "1. Scraping → Bronze (MinIO)",
            "2. Streaming → Kafka topic: news-raw",
            "3. Bronze → Silver (nettoyage ETL)",
            "4. Silver → Gold (enrichissement TF-IDF)",
            "5. Gold → DWH PostgreSQL",
            "6. Contrôles qualité",
            "7. Refresh vues matérialisées",
            "8. Log traçabilité",
        ],
    },
}


def get_lineage_report() -> dict:
    """Génère un rapport de lignage depuis le DWH."""
    try:
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
            user=DWH_USER, password=DWH_PASS
        )
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    dag_name, step, status,
                    SUM(articles_in) AS total_in,
                    SUM(articles_out) AS total_out,
                    COUNT(*) AS runs,
                    MAX(executed_at) AS last_run
                FROM fact_pipeline_logs
                GROUP BY dag_name, step, status
                ORDER BY last_run DESC
            """)
            logs = [dict(r) for r in cur.fetchall()]

            cur.execute("""
                SELECT s.name, s.country, s.language,
                       COUNT(a.article_id) AS total_articles,
                       MAX(a.ingested_at) AS last_ingested,
                       AVG(a.quality_score) AS avg_quality
                FROM dim_source s
                LEFT JOIN fact_articles a ON s.source_id = a.source_id
                GROUP BY s.name, s.country, s.language
                ORDER BY total_articles DESC
            """)
            sources = [dict(r) for r in cur.fetchall()]

        conn.close()
        return {
            "generated_at": datetime.utcnow().isoformat(),
            "pipeline_logs": logs,
            "sources_stats": sources,
            "catalog": DATA_CATALOG,
        }
    except Exception as e:
        logger.error(f"Lineage report erreur : {e}")
        return {"error": str(e), "catalog": DATA_CATALOG}


def print_catalog():
    """Affiche le catalogue de données."""
    print(json.dumps(DATA_CATALOG, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = get_lineage_report()
    print(json.dumps(report, ensure_ascii=False, indent=2, default=str))
