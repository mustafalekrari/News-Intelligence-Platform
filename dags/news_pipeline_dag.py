"""
DAG principal : Orchestration du pipeline news
- Scraping batch toutes les heures
- ETL Bronze → Silver → Gold
- Contrôles qualité
- Refresh vues DWH
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import logging
import sys
import os

sys.path.insert(0, "/opt/airflow/scrapers")
sys.path.insert(0, "/opt/airflow/etl")
sys.path.insert(0, "/opt/airflow/quality")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "news-platform",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="news_pipeline",
    description="Pipeline complet : scraping → bronze → silver → gold → DWH",
    default_args=default_args,
    schedule_interval="0 * * * *",   # toutes les heures
    start_date=days_ago(1),
    catchup=False,
    max_active_runs=1,
    tags=["news", "etl", "pipeline"],
) as dag:

    # ── 1. Scraping ────────────────────────────────────────
    def task_scrape(**ctx):
        from hespress import scrape_hespress
        from rss_scraper import scrape_rss_sources
        from bbc import scrape_bbc
        from minio_storage import save_articles_to_bronze
        from kafka_producer import get_producer, send_articles
        import hashlib

        all_articles = []

        for fn, name in [(scrape_hespress, "Hespress"), (scrape_bbc, "BBC"), (scrape_rss_sources, "RSS")]:
            try:
                arts = fn()
                all_articles.extend(arts)
                logger.info(f"{name} : {len(arts)} articles")
            except Exception as e:
                logger.error(f"{name} ÉCHEC : {e}")

        # Déduplication
        seen, unique = set(), []
        for a in all_articles:
            h = a.get("url_hash", "")
            if h and h not in seen:
                seen.add(h)
                unique.append(a)

        logger.info(f"Total unique : {len(unique)}")

        saved = save_articles_to_bronze(unique)
        logger.info(f"Bronze sauvegardés : {saved}")

        producer = get_producer()
        if producer:
            send_articles(producer, unique)
            producer.close()

        ctx["ti"].xcom_push(key="articles_count", value=len(unique))
        return len(unique)

    # ── 2. ETL ─────────────────────────────────────────────
    def task_etl(**ctx):
        from worker import run_etl_pipeline
        run_etl_pipeline()

    # ── 3. Qualité ─────────────────────────────────────────
    def task_quality(**ctx):
        from quality_checks import run_quality_checks
        report = run_quality_checks()
        logger.info(f"Qualité : {report}")
        ctx["ti"].xcom_push(key="quality_report", value=report)

    # ── 4. Refresh DWH ─────────────────────────────────────
    def task_refresh_dwh(**ctx):
        import psycopg2
        conn = psycopg2.connect(
            host=os.getenv("DWH_HOST", "postgres-dwh"),
            port=int(os.getenv("DWH_PORT", 5432)),
            dbname=os.getenv("DWH_DB", "news_dwh"),
            user=os.getenv("DWH_USER", "news_user"),
            password=os.getenv("DWH_PASSWORD", "news_pass"),
        )
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_all_materialized_views()")
        conn.commit()
        conn.close()
        logger.info("Vues matérialisées rafraîchies")

    # ── 5. Log pipeline ────────────────────────────────────
    def task_log_pipeline(**ctx):
        import psycopg2
        ti = ctx["ti"]
        count = ti.xcom_pull(key="articles_count", task_ids="scraping") or 0
        conn = psycopg2.connect(
            host=os.getenv("DWH_HOST", "postgres-dwh"),
            port=int(os.getenv("DWH_PORT", 5432)),
            dbname=os.getenv("DWH_DB", "news_dwh"),
            user=os.getenv("DWH_USER", "news_user"),
            password=os.getenv("DWH_PASSWORD", "news_pass"),
        )
        with conn.cursor() as cur:
            cur.execute("""
                INSERT INTO fact_pipeline_logs
                    (run_id, dag_name, step, status, articles_in, articles_out, message)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, (
                ctx["run_id"], "news_pipeline", "full_pipeline",
                "success", count, count, f"Run {ctx['ds']}"
            ))
        conn.commit()
        conn.close()

    # ── Opérateurs ─────────────────────────────────────────
    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    scraping    = PythonOperator(task_id="scraping",    python_callable=task_scrape,      provide_context=True)
    etl         = PythonOperator(task_id="etl",         python_callable=task_etl,         provide_context=True)
    quality     = PythonOperator(task_id="quality",     python_callable=task_quality,     provide_context=True)
    refresh_dwh = PythonOperator(task_id="refresh_dwh", python_callable=task_refresh_dwh, provide_context=True)
    log_run     = PythonOperator(task_id="log_pipeline", python_callable=task_log_pipeline, provide_context=True)

    start >> scraping >> etl >> quality >> refresh_dwh >> log_run >> end
