from typing import List, Dict, Optional
"""
ETL Worker — Pipeline Bronze → Silver → Gold
Lit depuis MinIO, transforme, charge dans PostgreSQL DWH
"""
import json
import logging
import os
import re
import time
from datetime import datetime, date
from io import BytesIO

import psycopg2
import psycopg2.extras
from minio import Minio
from minio.error import S3Error
from langdetect import detect, LangDetectException
from sklearn.feature_extraction.text import TfidfVectorizer
import nltk

# ── NLTK data ──────────────────────────────────────────────
try:
    nltk.data.find("corpora/stopwords")
except LookupError:
    nltk.download("stopwords", quiet=True)
try:
    nltk.data.find("tokenizers/punkt")
except LookupError:
    nltk.download("punkt", quiet=True)

from nltk.corpus import stopwords

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s : %(message)s",
)
logger = logging.getLogger("etl-worker")

# ── Config ─────────────────────────────────────────────────
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
BUCKET_BRONZE  = os.getenv("MINIO_BUCKET_BRONZE", "bronze")
BUCKET_SILVER  = os.getenv("MINIO_BUCKET_SILVER", "silver")
BUCKET_GOLD    = os.getenv("MINIO_BUCKET_GOLD", "gold")

DWH_HOST = os.getenv("DWH_HOST", "postgres-dwh")
DWH_PORT = int(os.getenv("DWH_PORT", "5432"))
DWH_DB   = os.getenv("DWH_DB", "news_dwh")
DWH_USER = os.getenv("DWH_USER", "news_user")
DWH_PASS = os.getenv("DWH_PASSWORD", "news_pass")

ETL_INTERVAL = int(os.getenv("ETL_INTERVAL_MINUTES", "30"))

STOP_WORDS = set()
for lang in ["english", "french", "arabic"]:
    try:
        STOP_WORDS.update(stopwords.words(lang))
    except Exception:
        pass


# ── Clients ────────────────────────────────────────────────

def get_minio() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS, secret_key=MINIO_SECRET, secure=False)


def get_dwh_conn():
    return psycopg2.connect(
        host=DWH_HOST, port=DWH_PORT, dbname=DWH_DB,
        user=DWH_USER, password=DWH_PASS
    )


# ── Bronze → Silver ────────────────────────────────────────

def clean_text(text: str) -> str:
    """Supprime HTML, normalise les espaces."""
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"&[a-z]+;", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def detect_language(text: str) -> str:
    try:
        return detect(text[:500])
    except LangDetectException:
        return "unknown"


def bronze_to_silver(article: dict) -> Optional[Dict]:
    """Nettoie et normalise un article brut."""
    title = clean_text(article.get("title", ""))
    content = clean_text(article.get("content", ""))

    if not title or len(content) < 100:
        return None

    lang = article.get("language") or detect_language(content)

    return {
        **article,
        "title": title,
        "content": content,
        "language": lang,
        "word_count": len(content.split()),
        "content_preview": content[:500],
        "silver_processed_at": datetime.utcnow().isoformat(),
    }


# ── Silver → Gold (TF-IDF keywords) ───────────────────────

def extract_keywords(texts: List[str], top_n: int = 10) -> List[List[tuple]]:
    """Extrait les top N mots-clés par TF-IDF pour une liste de textes."""
    if not texts:
        return []
    try:
        vec = TfidfVectorizer(
            max_features=500,
            stop_words=list(STOP_WORDS) if STOP_WORDS else None,
            min_df=1,
        )
        matrix = vec.fit_transform(texts)
        feature_names = vec.get_feature_names_out()
        results = []
        for row in matrix:
            scores = zip(feature_names, row.toarray()[0])
            top = sorted(scores, key=lambda x: x[1], reverse=True)[:top_n]
            results.append([(w, s) for w, s in top if s > 0])
        return results
    except Exception as e:
        logger.warning(f"TF-IDF erreur : {e}")
        return [[] for _ in texts]


# ── DWH Loaders ────────────────────────────────────────────

def get_or_create_date(cur, dt: date) -> int:
    cur.execute("SELECT date_id FROM dim_date WHERE full_date = %s", (dt,))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute("""
        INSERT INTO dim_date (full_date, day, week, month, quarter, year, day_name, is_weekend)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s) RETURNING date_id
    """, (
        dt, dt.day, dt.isocalendar()[1], dt.month,
        (dt.month - 1) // 3 + 1, dt.year,
        dt.strftime("%A"), dt.weekday() >= 5
    ))
    return cur.fetchone()[0]


def get_source_id(cur, source_name: str) -> Optional[int]:
    cur.execute("SELECT source_id FROM dim_source WHERE name = %s", (source_name,))
    row = cur.fetchone()
    return row[0] if row else None


def get_language_id(cur, code: str) -> Optional[int]:
    cur.execute("SELECT language_id FROM dim_language WHERE code = %s", (code,))
    row = cur.fetchone()
    return row[0] if row else None


def get_or_create_category(cur, name: str, language: str) -> int:
    cur.execute("SELECT category_id FROM dim_category WHERE name = %s AND language = %s", (name, language))
    row = cur.fetchone()
    if row:
        return row[0]
    cur.execute(
        "INSERT INTO dim_category (name, language) VALUES (%s, %s) RETURNING category_id",
        (name, language)
    )
    return cur.fetchone()[0]


def load_article_to_dwh(conn, article: dict, keywords: List[tuple]) -> bool:
    """Insère un article silver dans le DWH (gold layer)."""
    try:
        with conn.cursor() as cur:
            # Date
            pub_str = article.get("published_at", "")
            try:
                pub_dt = datetime.fromisoformat(pub_str[:19])
            except Exception:
                pub_dt = datetime.utcnow()

            date_id = get_or_create_date(cur, pub_dt.date())
            source_id = get_source_id(cur, article.get("source", ""))
            lang_id = get_language_id(cur, article.get("language", "en")[:2])
            cat_id = get_or_create_category(
                cur,
                article.get("category", "general"),
                article.get("language", "en")[:2]
            )

            word_count = article.get("word_count", len(article.get("content", "").split()))
            quality = _compute_quality(article)

            # Upsert article
            cur.execute("""
                INSERT INTO fact_articles
                    (url_hash, title, author, content_preview, word_count,
                     date_id, source_id, category_id, language_id,
                     published_at, layer, quality_score, minio_path)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,'gold',%s,%s)
                ON CONFLICT (url_hash) DO UPDATE SET
                    layer = 'gold',
                    quality_score = EXCLUDED.quality_score,
                    minio_path = EXCLUDED.minio_path
                RETURNING article_id
            """, (
                article.get("url_hash"), article.get("title"), article.get("author"),
                article.get("content_preview"), word_count,
                date_id, source_id, cat_id, lang_id,
                pub_dt, quality, article.get("minio_path")
            ))
            row = cur.fetchone()
            if not row:
                return False
            article_id = row[0]

            # Keywords
            for word, score in keywords:
                cur.execute("""
                    INSERT INTO fact_keywords (article_id, word, tfidf_score, frequency, date_id)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (article_id, word, float(score), 1, date_id))

        conn.commit()
        return True
    except Exception as e:
        conn.rollback()
        logger.error(f"DWH insert erreur : {e}")
        return False


def _compute_quality(article: dict) -> float:
    """Score qualité 0.0–1.0 basé sur la complétude."""
    score = 0.0
    if article.get("title") and len(article["title"]) > 10:
        score += 0.25
    if article.get("content") and len(article["content"]) > 200:
        score += 0.35
    if article.get("author"):
        score += 0.15
    if article.get("published_at"):
        score += 0.15
    if article.get("category"):
        score += 0.10
    return round(score, 2)


# ── Pipeline principal ─────────────────────────────────────

def run_etl_pipeline():
    logger.info("=" * 60)
    logger.info(f"ETL PIPELINE DÉMARRÉ — {datetime.utcnow().isoformat()}")

    minio = get_minio()
    start = datetime.utcnow()
    stats = {"read": 0, "silver": 0, "gold": 0, "errors": 0}

    # Lire les articles bronze du jour
    today = datetime.utcnow()
    prefix = f"{today.year}/{today.month:02d}/{today.day:02d}/"

    try:
        objects = list(minio.list_objects(BUCKET_BRONZE, prefix=prefix, recursive=True))
    except S3Error as e:
        logger.error(f"MinIO lecture bronze : {e}")
        return

    logger.info(f"Bronze : {len(objects)} fichiers trouvés pour {prefix}")

    silver_articles = []
    for obj in objects:
        try:
            response = minio.get_object(BUCKET_BRONZE, obj.object_name)
            raw = json.loads(response.read().decode("utf-8"))
            stats["read"] += 1

            silver = bronze_to_silver(raw)
            if silver:
                silver_articles.append(silver)
                stats["silver"] += 1

                # Sauvegarder en Silver
                silver_path = obj.object_name
                data = json.dumps(silver, ensure_ascii=False).encode("utf-8")
                minio.put_object(BUCKET_SILVER, silver_path, BytesIO(data), len(data),
                                 content_type="application/json")
        except Exception as e:
            stats["errors"] += 1
            logger.warning(f"Erreur traitement {obj.object_name} : {e}")

    logger.info(f"Silver : {stats['silver']}/{stats['read']} articles nettoyés")

    if not silver_articles:
        logger.warning("Aucun article silver — pipeline terminé")
        return

    # TF-IDF sur tous les articles
    texts = [a.get("content", "") for a in silver_articles]
    all_keywords = extract_keywords(texts, top_n=10)

    # Charger dans DWH
    try:
        conn = get_dwh_conn()
        for article, kws in zip(silver_articles, all_keywords):
            # Sauvegarder en Gold
            gold_path = f"gold/{article.get('source','unknown').lower().replace(' ','_')}/{article.get('url_hash','')}.json"
            article["minio_path"] = f"{BUCKET_GOLD}/{gold_path}"
            gold_data = json.dumps({**article, "keywords": kws}, ensure_ascii=False, default=str).encode("utf-8")
            try:
                minio.put_object(BUCKET_GOLD, gold_path, BytesIO(gold_data), len(gold_data),
                                 content_type="application/json")
            except Exception:
                pass

            if load_article_to_dwh(conn, article, kws):
                stats["gold"] += 1

        # Refresh vues matérialisées
        with conn.cursor() as cur:
            cur.execute("SELECT refresh_all_materialized_views()")
        conn.commit()
        conn.close()
    except Exception as e:
        logger.error(f"DWH connexion erreur : {e}")

    duration = (datetime.utcnow() - start).total_seconds()
    logger.info(f"ETL TERMINÉ — bronze:{stats['read']} silver:{stats['silver']} gold:{stats['gold']} erreurs:{stats['errors']} ({duration:.1f}s)")
    logger.info("=" * 60)


def main():
    logger.info("ETL Worker démarré")
    logger.info(f"Intervalle : {ETL_INTERVAL} minutes")

    # Premier run immédiat
    try:
        run_etl_pipeline()
    except Exception as e:
        logger.error(f"Pipeline erreur : {e}")

    while True:
        time.sleep(ETL_INTERVAL * 60)
        try:
            run_etl_pipeline()
        except Exception as e:
            logger.error(f"Pipeline erreur : {e}")


if __name__ == "__main__":
    main()
