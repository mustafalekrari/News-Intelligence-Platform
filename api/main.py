"""
API FastAPI — expose les données du DWH pour le dashboard
"""
from fastapi import FastAPI, Query
from fastapi.middleware.cors import CORSMiddleware
import psycopg2
import psycopg2.extras
import os
from datetime import datetime, timedelta

app = FastAPI(title="News Platform API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_conn():
    return psycopg2.connect(
        host=os.getenv("DWH_HOST", "postgres-dwh"),
        port=int(os.getenv("DWH_PORT", 5432)),
        dbname=os.getenv("DWH_DB", "news_dwh"),
        user=os.getenv("DWH_USER", "news_user"),
        password=os.getenv("DWH_PASSWORD", "news_pass"),
    )

def query(sql: str, params=None) -> list[dict]:
    conn = get_conn()
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(sql, params)
        rows = [dict(r) for r in cur.fetchall()]
    conn.close()
    return rows


# ── Stats globales ─────────────────────────────────────────
@app.get("/api/stats")
def get_stats():
    rows = query("""
        SELECT
            COUNT(*) AS total_articles,
            COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '24 hours') AS articles_24h,
            COUNT(*) FILTER (WHERE ingested_at >= NOW() - INTERVAL '1 hour')  AS articles_1h,
            ROUND(AVG(quality_score)::numeric, 2) AS avg_quality,
            COUNT(DISTINCT source_id) AS active_sources
        FROM fact_articles
    """)
    return rows[0] if rows else {}


# ── Articles par jour ──────────────────────────────────────
@app.get("/api/articles-per-day")
def articles_per_day(days: int = Query(7, ge=1, le=30)):
    return query("""
        SELECT full_date::text AS date, SUM(nb_articles) AS count
        FROM mv_articles_par_jour
        WHERE full_date >= NOW() - INTERVAL '%s days'
        GROUP BY full_date ORDER BY full_date
    """, (days,))


# ── Articles par source ────────────────────────────────────
@app.get("/api/articles-per-source")
def articles_per_source(days: int = Query(7, ge=1, le=30)):
    return query("""
        SELECT source, SUM(nb_articles) AS count, country
        FROM mv_articles_par_jour
        WHERE full_date >= NOW() - INTERVAL '%s days'
        GROUP BY source, country ORDER BY count DESC
    """, (days,))


# ── Top mots-clés ──────────────────────────────────────────
@app.get("/api/top-keywords")
def top_keywords(days: int = Query(3, ge=1, le=14), limit: int = Query(15, ge=5, le=50)):
    return query("""
        SELECT word, SUM(total_freq) AS frequency, COUNT(DISTINCT nb_articles) AS articles
        FROM mv_top_keywords_jour
        WHERE full_date >= NOW() - INTERVAL '%s days'
        GROUP BY word ORDER BY frequency DESC LIMIT %s
    """, (days, limit))


# ── Derniers articles ──────────────────────────────────────
@app.get("/api/latest-articles")
def latest_articles(limit: int = Query(20, ge=5, le=100)):
    return query("""
        SELECT
            a.title, a.author, a.content_preview,
            s.name AS source, s.country,
            l.code AS language,
            c.name AS category,
            a.published_at, a.quality_score, a.word_count
        FROM fact_articles a
        JOIN dim_source s ON a.source_id = s.source_id
        LEFT JOIN dim_language l ON a.language_id = l.language_id
        LEFT JOIN dim_category c ON a.category_id = c.category_id
        ORDER BY a.ingested_at DESC LIMIT %s
    """, (limit,))


# ── Santé du pipeline ──────────────────────────────────────
@app.get("/api/pipeline-health")
def pipeline_health():
    return query("""
        SELECT dag_name, step, success_count, failed_count,
               ROUND(avg_duration_sec::numeric, 1) AS avg_duration_sec,
               total_articles_in, total_articles_out, last_run::text
        FROM mv_pipeline_health ORDER BY last_run DESC
    """)


# ── Tendances par langue ───────────────────────────────────
@app.get("/api/articles-per-language")
def articles_per_language(days: int = Query(7)):
    return query("""
        SELECT language, SUM(nb_articles) AS count
        FROM mv_articles_par_jour
        WHERE full_date >= NOW() - INTERVAL '%s days'
        GROUP BY language ORDER BY count DESC
    """, (days,))


# ── Qualité distribution ───────────────────────────────────
@app.get("/api/quality-distribution")
def quality_distribution():
    return query("""
        SELECT
            CASE
                WHEN quality_score >= 0.8 THEN 'Excellent'
                WHEN quality_score >= 0.6 THEN 'Bon'
                WHEN quality_score >= 0.4 THEN 'Moyen'
                ELSE 'Faible'
            END AS level,
            COUNT(*) AS count
        FROM fact_articles
        WHERE ingested_at >= NOW() - INTERVAL '7 days'
        GROUP BY level ORDER BY count DESC
    """)
