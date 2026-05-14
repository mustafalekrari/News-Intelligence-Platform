-- ============================================================
--  NEWS DWH — Schéma en Étoile (Star Schema)
--  PostgreSQL 15 | Initialisation automatique au démarrage
-- ============================================================

-- Extension pour UUID
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
CREATE EXTENSION IF NOT EXISTS pg_trgm;  -- recherche full-text

-- ─────────────────────────────────────────
--  DIMENSIONS
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS dim_date (
    date_id     SERIAL PRIMARY KEY,
    full_date   DATE NOT NULL UNIQUE,
    day         INT,
    week        INT,
    month       INT,
    quarter     INT,
    year        INT,
    day_name    VARCHAR(10),
    is_weekend  BOOLEAN
);

CREATE TABLE IF NOT EXISTS dim_source (
    source_id   SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL UNIQUE,
    url         VARCHAR(255),
    country     VARCHAR(50),
    language    VARCHAR(10),
    category    VARCHAR(50),
    created_at  TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS dim_category (
    category_id SERIAL PRIMARY KEY,
    name        VARCHAR(100) NOT NULL,
    parent      VARCHAR(100),
    language    VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS dim_language (
    language_id SERIAL PRIMARY KEY,
    code        VARCHAR(10) NOT NULL UNIQUE,   -- 'ar', 'fr', 'en'
    name        VARCHAR(50)
);

-- ─────────────────────────────────────────
--  FACTS
-- ─────────────────────────────────────────

CREATE TABLE IF NOT EXISTS fact_articles (
    article_id      UUID DEFAULT uuid_generate_v4() PRIMARY KEY,
    url_hash        VARCHAR(64) UNIQUE NOT NULL,   -- SHA256 de l'URL
    title           TEXT NOT NULL,
    author          VARCHAR(255),
    content_preview TEXT,                          -- 500 premiers chars
    word_count      INT,
    date_id         INT REFERENCES dim_date(date_id),
    source_id       INT REFERENCES dim_source(source_id),
    category_id     INT REFERENCES dim_category(category_id),
    language_id     INT REFERENCES dim_language(language_id),
    published_at    TIMESTAMP,
    ingested_at     TIMESTAMP DEFAULT NOW(),
    layer           VARCHAR(10) DEFAULT 'gold',    -- bronze/silver/gold
    quality_score   FLOAT,                         -- 0.0 à 1.0
    minio_path      TEXT                           -- chemin dans MinIO
);

CREATE TABLE IF NOT EXISTS fact_keywords (
    keyword_id  SERIAL PRIMARY KEY,
    article_id  UUID REFERENCES fact_articles(article_id) ON DELETE CASCADE,
    word        VARCHAR(100) NOT NULL,
    tfidf_score FLOAT,
    frequency   INT,
    date_id     INT REFERENCES dim_date(date_id)
);

CREATE TABLE IF NOT EXISTS fact_pipeline_logs (
    log_id          SERIAL PRIMARY KEY,
    run_id          VARCHAR(100),
    dag_name        VARCHAR(100),
    step            VARCHAR(50),   -- 'scraping','bronze_to_silver','silver_to_gold'
    status          VARCHAR(20),   -- 'success','failed','warning'
    articles_in     INT DEFAULT 0,
    articles_out    INT DEFAULT 0,
    errors_count    INT DEFAULT 0,
    duration_sec    FLOAT,
    message         TEXT,
    executed_at     TIMESTAMP DEFAULT NOW()
);

-- ─────────────────────────────────────────
--  VUES ANALYTIQUES MATÉRIALISÉES
-- ─────────────────────────────────────────

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_articles_par_jour AS
SELECT
    d.full_date,
    d.year,
    d.month,
    d.week,
    s.name AS source,
    s.country,
    l.code AS language,
    COUNT(a.article_id) AS nb_articles,
    AVG(a.word_count) AS avg_word_count,
    AVG(a.quality_score) AS avg_quality
FROM fact_articles a
JOIN dim_date d ON a.date_id = d.date_id
JOIN dim_source s ON a.source_id = s.source_id
LEFT JOIN dim_language l ON a.language_id = l.language_id
GROUP BY d.full_date, d.year, d.month, d.week, s.name, s.country, l.code
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_top_keywords_jour AS
SELECT
    d.full_date,
    k.word,
    s.name AS source,
    SUM(k.frequency) AS total_freq,
    AVG(k.tfidf_score) AS avg_tfidf,
    COUNT(DISTINCT k.article_id) AS nb_articles
FROM fact_keywords k
JOIN fact_articles a ON k.article_id = a.article_id
JOIN dim_date d ON k.date_id = d.date_id
JOIN dim_source s ON a.source_id = s.source_id
GROUP BY d.full_date, k.word, s.name
ORDER BY d.full_date DESC, total_freq DESC
WITH DATA;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_pipeline_health AS
SELECT
    dag_name,
    step,
    COUNT(*) FILTER (WHERE status = 'success') AS success_count,
    COUNT(*) FILTER (WHERE status = 'failed') AS failed_count,
    AVG(duration_sec) AS avg_duration_sec,
    SUM(articles_in) AS total_articles_in,
    SUM(articles_out) AS total_articles_out,
    MAX(executed_at) AS last_run
FROM fact_pipeline_logs
WHERE executed_at >= NOW() - INTERVAL '7 days'
GROUP BY dag_name, step
WITH DATA;

-- ─────────────────────────────────────────
--  INDEX pour les performances
-- ─────────────────────────────────────────

CREATE INDEX IF NOT EXISTS idx_articles_date ON fact_articles(date_id);
CREATE INDEX IF NOT EXISTS idx_articles_source ON fact_articles(source_id);
CREATE INDEX IF NOT EXISTS idx_articles_published ON fact_articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_url_hash ON fact_articles(url_hash);
CREATE INDEX IF NOT EXISTS idx_keywords_word ON fact_keywords(word);
CREATE INDEX IF NOT EXISTS idx_keywords_date ON fact_keywords(date_id);

-- ─────────────────────────────────────────
--  DONNÉES DE RÉFÉRENCE
-- ─────────────────────────────────────────

INSERT INTO dim_language (code, name) VALUES
    ('ar', 'Arabic'),
    ('fr', 'French'),
    ('en', 'English'),
    ('es', 'Spanish')
ON CONFLICT (code) DO NOTHING;

INSERT INTO dim_source (name, url, country, language, category) VALUES
    ('Hespress', 'https://www.hespress.com', 'MA', 'ar', 'news'),
    ('Akhbarona', 'https://www.akhbarona.com', 'MA', 'ar', 'news'),
    ('Barlamane', 'https://www.barlamane.com', 'MA', 'ar', 'news'),
    ('BBC News', 'https://www.bbc.com/news', 'GB', 'en', 'news'),
    ('Al Jazeera', 'https://www.aljazeera.com', 'QA', 'en', 'news'),
    ('Reuters', 'https://www.reuters.com', 'GB', 'en', 'news'),
    ('CNN', 'https://www.cnn.com', 'US', 'en', 'news')
ON CONFLICT (name) DO NOTHING;

-- ─────────────────────────────────────────
--  FONCTION utilitaire : refresh des vues
-- ─────────────────────────────────────────

CREATE OR REPLACE FUNCTION refresh_all_materialized_views()
RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_articles_par_jour;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_top_keywords_jour;
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_pipeline_health;
END;
$$ LANGUAGE plpgsql;

-- Index unique requis pour REFRESH CONCURRENTLY
CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_articles_uniq
    ON mv_articles_par_jour(full_date, source);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_keywords_uniq
    ON mv_top_keywords_jour(full_date, word, source);

CREATE UNIQUE INDEX IF NOT EXISTS idx_mv_pipeline_health_uniq
    ON mv_pipeline_health(dag_name, step);

COMMENT ON TABLE fact_articles IS 'Table de faits principale — un enregistrement par article collecté';
COMMENT ON TABLE fact_keywords IS 'Mots-clés extraits par TF-IDF pour chaque article';
COMMENT ON TABLE fact_pipeline_logs IS 'Logs de traçabilité du pipeline ETL';

SELECT 'Schéma DWH initialisé avec succès ✓' AS status;
