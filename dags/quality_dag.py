"""
DAG qualité — Contrôles quotidiens sur les données silver
Planifié à 2h du matin chaque jour
"""
from datetime import timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
import sys
import logging

sys.path.insert(0, "/opt/airflow/quality")

logger = logging.getLogger(__name__)

default_args = {
    "owner": "news-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="news_quality_checks",
    description="Contrôles qualité quotidiens — complétude, cohérence, validité",
    default_args=default_args,
    schedule_interval="0 2 * * *",   # 2h du matin
    start_date=days_ago(1),
    catchup=False,
    tags=["quality", "governance"],
) as dag:

    def run_checks(**ctx):
        from quality_checks import run_quality_checks
        report = run_quality_checks()
        logger.info(f"Rapport qualité : {report}")

        # Alerter si trop d'articles invalides
        total = report.get("total", 0)
        invalid = report.get("invalid", 0)
        if total > 0 and (invalid / total) > 0.3:
            logger.warning(f"ALERTE : {invalid}/{total} articles invalides ({100*invalid//total}%)")

        return report

    start = EmptyOperator(task_id="start")
    end   = EmptyOperator(task_id="end")

    quality = PythonOperator(
        task_id="quality_checks",
        python_callable=run_checks,
        provide_context=True,
    )

    start >> quality >> end
