from typing import List, Dict, Optional
"""
Qualité des données — Contrôles sur les articles silver/gold
Dimensions : Complétude, Cohérence, Validité
"""
import json
import logging
import os
from datetime import datetime
from io import BytesIO

import psycopg2
from minio import Minio
from minio.error import S3Error

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_SILVER  = os.getenv("MINIO_BUCKET_SILVER", "silver")
BUCKET_REPORTS = "quality-reports"

DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB   = os.getenv("DWH_DB", "news_dwh")
DWH_USER = os.getenv("DWH_USER", "news_user")
DWH_PASS = os.getenv("DWH_PASSWORD", "news_pass")

MIN_CONTENT_LENGTH = 100
MIN_TITLE_LENGTH   = 5


# ── Règles de qualité ──────────────────────────────────────

def check_completeness(article: dict) -> dict:
    """Vérifie la complétude des champs obligatoires."""
    issues = []
    if not article.get("title") or len(article.get("title", "")) < MIN_TITLE_LENGTH:
        issues.append("missing_title")
    if not article.get("content") or len(article.get("content", "")) < MIN_CONTENT_LENGTH:
        issues.append("content_too_short")
    if not article.get("published_at"):
        issues.append("missing_date")
    if not article.get("source"):
        issues.append("missing_source")
    if not article.get("url"):
        issues.append("missing_url")
    return {"dimension": "completeness", "issues": issues, "passed": len(issues) == 0}


def check_coherence(article: dict) -> dict:
    """Vérifie la cohérence des données."""
    issues = []

    # Date dans le futur
    pub = article.get("published_at", "")
    if pub:
        try:
            pub_dt = datetime.fromisoformat(pub[:19])
            if pub_dt > datetime.utcnow():
                issues.append("future_date")
        except Exception:
            issues.append("invalid_date_format")

    # Langue incohérente avec la source
    source = article.get("source", "")
    lang   = article.get("language", "")
    arabic_sources = {"Hespress", "Akhbarona", "Barlamane"}
    if source in arabic_sources and lang not in ("ar", "unknown"):
        issues.append(f"language_mismatch:{source}={lang}")

    # Titre trop long (probablement du bruit)
    if len(article.get("title", "")) > 500:
        issues.append("title_too_long")

    return {"dimension": "coherence", "issues": issues, "passed": len(issues) == 0}


def check_validity(article: dict) -> dict:
    """Vérifie la validité des formats."""
    import re
    issues = []

    url = article.get("url", "")
    if url and not re.match(r"https?://", url):
        issues.append("invalid_url_format")

    url_hash = article.get("url_hash", "")
    if url_hash and len(url_hash) != 64:
        issues.append("invalid_url_hash")

    word_count = article.get("word_count", 0)
    if word_count and word_count < 0:
        issues.append("negative_word_count")

    return {"dimension": "validity", "issues": issues, "passed": len(issues) == 0}


def compute_quality_score(checks: List[Dict]) -> float:
    """Score global 0.0–1.0 basé sur les dimensions."""
    weights = {"completeness": 0.5, "coherence": 0.3, "validity": 0.2}
    score = 0.0
    for check in checks:
        dim = check["dimension"]
        if check["passed"]:
            score += weights.get(dim, 0.1)
    return round(score, 2)


def run_checks_on_article(article: dict) -> dict:
    """Lance tous les contrôles sur un article."""
    checks = [
        check_completeness(article),
        check_coherence(article),
        check_validity(article),
    ]
    score = compute_quality_score(checks)
    all_issues = [i for c in checks for i in c["issues"]]
    return {
        "url_hash": article.get("url_hash", ""),
        "source": article.get("source", ""),
        "checks": checks,
        "quality_score": score,
        "issues": all_issues,
        "is_valid": score >= 0.5,
    }


# ── Pipeline qualité ───────────────────────────────────────

def run_quality_checks() -> dict:
    """Lance les contrôles qualité sur les articles silver du jour."""
    minio = Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)
    today = datetime.utcnow()
    prefix = f"{today.year}/{today.month:02d}/{today.day:02d}/"

    stats = {
        "total": 0, "valid": 0, "invalid": 0,
        "issues_summary": {},
        "checked_at": today.isoformat(),
    }

    results = []
    try:
        objects = list(minio.list_objects(BUCKET_SILVER, prefix=prefix, recursive=True))
    except S3Error as e:
        logger.error(f"MinIO silver lecture : {e}")
        return stats

    for obj in objects:
        try:
            resp = minio.get_object(BUCKET_SILVER, obj.object_name)
            article = json.loads(resp.read().decode("utf-8"))
            result = run_checks_on_article(article)
            results.append(result)
            stats["total"] += 1

            if result["is_valid"]:
                stats["valid"] += 1
            else:
                stats["invalid"] += 1

            for issue in result["issues"]:
                stats["issues_summary"][issue] = stats["issues_summary"].get(issue, 0) + 1

        except Exception as e:
            logger.warning(f"Erreur qualité {obj.object_name} : {e}")

    # Sauvegarder le rapport
    report = {**stats, "details": results[:50]}  # max 50 détails
    report_path = f"{today.year}/{today.month:02d}/{today.day:02d}/quality_report_{today.strftime('%H%M%S')}.json"
    try:
        data = json.dumps(report, ensure_ascii=False, indent=2).encode("utf-8")
        minio.put_object(BUCKET_REPORTS, report_path, BytesIO(data), len(data),
                         content_type="application/json")
        logger.info(f"Rapport qualité sauvegardé : {report_path}")
    except Exception as e:
        logger.warning(f"Sauvegarde rapport : {e}")

    # Mettre à jour les scores dans le DWH
    _update_quality_scores_in_dwh(results)

    logger.info(f"Qualité : {stats['valid']}/{stats['total']} valides | issues: {stats['issues_summary']}")
    return stats


def _update_quality_scores_in_dwh(results: List[Dict]):
    """Met à jour les quality_score dans fact_articles."""
    if not results:
        return
    try:
        conn = psycopg2.connect(
            host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
            user=DWH_USER, password=DWH_PASS
        )
        with conn.cursor() as cur:
            for r in results:
                cur.execute(
                    "UPDATE fact_articles SET quality_score = %s WHERE url_hash = %s",
                    (r["quality_score"], r["url_hash"])
                )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DWH quality update : {e}")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    report = run_quality_checks()
    print(json.dumps(report, indent=2, ensure_ascii=False))
