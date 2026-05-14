# Cahier des Charges — News Intelligence Platform

---

## 1. Contexte et objectif

Le projet est une plateforme Big Data complète qui collecte automatiquement des articles de presse depuis des sites d'actualité marocains et internationaux, les transforme, les analyse et les visualise en temps réel.

L'objectif est de répondre à cette question : **parmi des milliers d'articles publiés chaque jour, quels sont les sujets qui dominent l'actualité ?**

---

## 2. Architecture globale

Le projet suit une architecture en couches appelée **Lambda Architecture** combinée avec une **Architecture Médaillon** :

```
[SOURCES WEB]
     ↓
[INGESTION] ──── Batch (toutes les heures) ────→ [MinIO Bronze]
     └─────────── Streaming (temps réel) ──────→ [Kafka]
                                                      ↓
[ETL] Bronze → Silver → Gold ──────────────────→ [PostgreSQL DWH]
                                                      ↓
[ORCHESTRATION Airflow] ────────────────────────→ [Dashboard + Grafana]
```

---

## 3. Composants détaillés

### 3.1 Scrapers (`/scrapers`)

**Rôle** : collecter les articles depuis les sites web.

| Fichier | Source | Langue | Méthode |
|---|---|---|---|
| `hespress.py` | Hespress.com | Arabe | HTML scraping |
| `akhbarona.py` | Akhbarona.com | Arabe | HTML scraping |
| `bbc.py` | BBC News | Anglais | HTML scraping |
| `rss_scraper.py` | Al Jazeera, Reuters, CNN | Anglais | Flux RSS |
| `kafka_producer.py` | — | — | Envoi Kafka |
| `minio_storage.py` | — | — | Stockage MinIO |
| `main.py` | — | — | Orchestrateur |

**Données collectées par article** :
```
url_hash       → identifiant unique (SHA256 de l'URL)
url            → lien de l'article
title          → titre
author         → auteur
content        → texte complet
category       → catégorie (politique, sport, etc.)
source         → nom du média
country        → pays (MA, GB, US, QA)
language       → langue (ar, en, fr)
published_at   → date de publication
scraped_at     → date de collecte
```

**Fonctionnement de `main.py`** :
1. Lance les 4 scrapers en séquence
2. Déduplique les articles par `url_hash` (SHA256 de l'URL)
3. Sauvegarde dans MinIO bronze
4. Envoie dans Kafka
5. Attend 60 minutes et recommence

---

### 3.2 Data Lake MinIO — Architecture Médaillon

**Rôle** : stocker toutes les données. MinIO est un équivalent open-source de Amazon S3.

| Bucket | Contenu | Transformation |
|---|---|---|
| `bronze` | Articles bruts JSON | Aucune — données exactes comme collectées |
| `silver` | Articles nettoyés | HTML supprimé, langue détectée, word_count calculé |
| `gold` | Articles enrichis | Mots-clés TF-IDF ajoutés |
| `quality-reports` | Rapports qualité JSON | Résultats des contrôles |

**Structure des chemins** :
```
bronze/YYYY/MM/DD/<source>/<url_hash>.json
silver/YYYY/MM/DD/<source>/<url_hash>.json
gold/<source>/<url_hash>.json
```

**Pourquoi garder le Bronze ?**
Si une transformation est mauvaise, on peut toujours repartir des données brutes originales. C'est le principe du Data Lake : ne jamais perdre la donnée source.

---

### 3.3 Kafka — Streaming temps réel

**Rôle** : transmettre chaque article comme un événement en temps réel.

| Composant | Rôle |
|---|---|
| Zookeeper | Gère le cluster Kafka (coordination) |
| Kafka broker | Reçoit et stocke les messages |
| Topic `news-raw` | Canal où transitent les articles |
| Kafka UI (port 8090) | Interface visuelle pour voir les messages |

**Pourquoi Kafka ?**
Kafka permet de traiter les articles immédiatement à leur publication, sans attendre le prochain cycle batch. C'est le **Speed Layer** de la Lambda Architecture.

---

### 3.4 ETL Worker (`/etl/worker.py`)

**Rôle** : transformer les données Bronze → Silver → Gold.

**Pipeline complet** :

```
1. Lecture Bronze (MinIO)
        ↓
2. Nettoyage (Bronze → Silver)
   - clean_text()     : supprime HTML, normalise espaces
   - detect_language(): identifie la langue automatiquement
   - Calcule word_count
   - Sauvegarde Silver dans MinIO
        ↓
3. Extraction mots-clés TF-IDF
   - TfidfVectorizer sur tous les articles du jour
   - Top 10 mots-clés par article
        ↓
4. Chargement DWH (Silver → Gold)
   - Sauvegarde Gold dans MinIO
   - Insère dans PostgreSQL (fact_articles + fact_keywords)
   - Calcule quality_score
        ↓
5. Refresh vues matérialisées PostgreSQL
```

**Qu'est-ce que TF-IDF ?**
TF-IDF (Term Frequency - Inverse Document Frequency) mesure l'importance d'un mot dans un article par rapport à tous les autres articles. Un mot qui apparaît souvent dans UN article mais rarement dans les autres a un score élevé → c'est un mot-clé pertinent.

**Intervalle** : toutes les 30 minutes.

---

### 3.5 Data Warehouse PostgreSQL (`/docker/postgres/init.sql`)

**Rôle** : stocker les données analytiques dans un schéma optimisé pour les requêtes.

**Schéma en étoile (Star Schema)** :

```
                    dim_date
                       │
dim_language ──── fact_articles ──── dim_source
                       │
                   dim_category
                       │
                  fact_keywords
                       │
               fact_pipeline_logs
```

**Tables de faits** :

| Table | Contenu |
|---|---|
| `fact_articles` | Un enregistrement par article collecté |
| `fact_keywords` | Mots-clés TF-IDF par article |
| `fact_pipeline_logs` | Traçabilité de chaque exécution |

**Tables de dimensions** :

| Table | Contenu |
|---|---|
| `dim_date` | Calendrier (jour, semaine, mois, trimestre, année, weekend) |
| `dim_source` | Médias (nom, URL, pays, langue) |
| `dim_language` | Langues (ar, en, fr, es) |
| `dim_category` | Catégories d'articles |

**Vues matérialisées** (résultats pré-calculés pour la performance) :

| Vue | Contenu |
|---|---|
| `mv_articles_par_jour` | Nombre d'articles par jour, source, langue |
| `mv_top_keywords_jour` | Top mots-clés par jour et par source |
| `mv_pipeline_health` | Santé du pipeline (taux de succès, durées) |

**Pourquoi un schéma en étoile ?**
Optimisé pour les requêtes analytiques (GROUP BY, COUNT, SUM). Les jointures sont simples car tout part de la table centrale `fact_articles`.

---

### 3.6 Orchestration Airflow (`/dags`)

**Rôle** : planifier et superviser tous les pipelines automatiquement.

**DAG 1 : `news_pipeline`**
- Schedule : toutes les heures (`0 * * * *`)
- Retries : 2 tentatives avec 5 minutes d'attente
```
start → scraping → etl → quality → refresh_dwh → log_pipeline → end
```

**DAG 2 : `news_quality_checks`**
- Schedule : tous les jours à 2h du matin (`0 2 * * *`)
```
start → quality_checks → end
```

**Fonctionnalités Airflow utilisées** :
- `PythonOperator` : exécute des fonctions Python
- `EmptyOperator` : marqueurs de début/fin
- XCom : passage de données entre tâches (ex: nombre d'articles)
- Logs par tâche : chaque étape est tracée individuellement

---

### 3.7 Qualité des données (`/quality/quality_checks.py`)

**Rôle** : vérifier que les données collectées sont fiables.

**3 dimensions de contrôle** :

| Dimension | Tests effectués |
|---|---|
| **Complétude** | Titre présent ? Contenu > 100 chars ? Date présente ? Source présente ? |
| **Cohérence** | Date dans le passé ? Langue cohérente avec la source ? Titre pas trop long ? |
| **Validité** | URL commence par http ? Hash fait 64 caractères ? Word count positif ? |

**Score qualité** :
```
Complétude  → 50% du score
Cohérence   → 30% du score
Validité    → 20% du score
Score final → 0.0 à 1.0
```

Un article avec score < 0.5 est considéré invalide. Si plus de 30% des articles sont invalides, une alerte est loggée.

---

### 3.8 Gouvernance (`/warehouse/governance.py`)

**Rôle** : documenter les données et assurer la traçabilité.

**Contenu** :
- **Catalogue de données** : description de chaque couche, chaque champ, chaque transformation appliquée.
- **Rapport de lignage** : d'où viennent les données, quelles transformations ont été appliquées, statistiques par source.

**Pourquoi la gouvernance ?**
Dans un projet Big Data professionnel, il faut savoir à tout moment : d'où vient cette donnée ? Qui l'a transformée ? Quand ? C'est la traçabilité du pipeline.

---

### 3.9 API FastAPI (`/api/main.py`)

**Rôle** : exposer les données du DWH pour le dashboard.

| Endpoint | Description |
|---|---|
| `GET /api/stats` | KPIs globaux (total, 24h, qualité, sources) |
| `GET /api/articles-per-day?days=7` | Articles par jour sur N jours |
| `GET /api/articles-per-source` | Répartition par média |
| `GET /api/top-keywords?days=3&limit=15` | Top mots-clés |
| `GET /api/latest-articles?limit=20` | Derniers articles collectés |
| `GET /api/pipeline-health` | Santé du pipeline |
| `GET /api/articles-per-language` | Répartition par langue |
| `GET /api/quality-distribution` | Distribution des scores qualité |

Documentation interactive : `http://localhost:8000/docs`

---

### 3.10 Dashboard (`/dashboard/index.html`)

**Rôle** : visualiser toutes les données en temps réel.

**Technologies** : HTML + JavaScript + Chart.js + Tailwind CSS

**Panels** :

| Panel | Type | Données |
|---|---|---|
| Total articles | KPI card | `fact_articles` COUNT |
| Articles 24h | KPI card | Filtre `ingested_at` |
| Score qualité | KPI card | AVG `quality_score` |
| Sources actives | KPI card | COUNT DISTINCT sources |
| Tendances | Line chart | `mv_articles_par_jour` |
| Par source | Donut chart | `mv_articles_par_jour` GROUP BY source |
| Top mots-clés | Progress bars | `mv_top_keywords_jour` |
| Par langue | Pie chart | `mv_articles_par_jour` GROUP BY language |
| Qualité | Bar chart | `fact_articles` GROUP BY score range |
| Pipeline health | Cards | `mv_pipeline_health` |
| Derniers articles | Table | `fact_articles` ORDER BY ingested_at |

**Auto-refresh** : toutes les 60 secondes.
**Mode démo** : si l'API est indisponible, affiche des données simulées.

---

## 4. Infrastructure Docker

| Service | Image | Port exposé | Rôle |
|---|---|---|---|
| zookeeper | confluentinc/cp-zookeeper:7.5.0 | — | Coordination Kafka |
| kafka | confluentinc/cp-kafka:7.5.0 | 9092 | Broker de messages |
| kafka-ui | provectuslabs/kafka-ui | 8090 | Interface Kafka |
| minio | minio/minio | 9000, 9001 | Data Lake |
| minio-init | minio/mc | — | Création des buckets |
| postgres-dwh | postgres:15-alpine | 5433 | Data Warehouse |
| postgres-airflow | postgres:15-alpine | — | Base Airflow |
| redis | redis:7-alpine | 6379 | Cache Airflow |
| airflow-init | apache/airflow:2.8.1 | — | Initialisation |
| airflow-webserver | apache/airflow:2.8.1 | 8080 | Interface Airflow |
| airflow-scheduler | apache/airflow:2.8.1 | — | Planificateur |
| scraper | Python custom | — | Collecte articles |
| etl-worker | Python custom | — | Transformation |
| api | FastAPI custom | 8000 | API REST |
| dashboard | nginx | 80 | Interface web |
| grafana | grafana/grafana:10.2.0 | 3000 | Dashboards avancés |

---

## 5. Flux de données complet (end-to-end)

```
┌─────────────────────────────────────────────────────────────┐
│  ÉTAPE 1 — COLLECTE (toutes les heures)                     │
│  Scraper visite Hespress, Akhbarona, BBC, CNN,              │
│  Al Jazeera, Reuters                                        │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  ÉTAPE 2 — STOCKAGE BRUT                                    │
│  Chaque article → JSON dans MinIO bronze                    │
│  Chaque article → Événement dans Kafka topic news-raw       │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  ÉTAPE 3 — NETTOYAGE (ETL Worker, toutes les 30 min)        │
│  Bronze → suppression HTML, détection langue → Silver       │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  ÉTAPE 4 — ENRICHISSEMENT                                   │
│  Silver → TF-IDF mots-clés, quality_score → Gold            │
│  Gold → chargement PostgreSQL DWH                           │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  ÉTAPE 5 — AGRÉGATION                                       │
│  Refresh vues matérialisées PostgreSQL                      │
│  (mv_articles_par_jour, mv_top_keywords_jour)               │
└──────────────────────────┬──────────────────────────────────┘
                           ↓
┌─────────────────────────────────────────────────────────────┐
│  ÉTAPE 6 — VISUALISATION                                    │
│  API FastAPI lit le DWH                                     │
│  Dashboard affiche tendances, mots-clés, qualité            │
└─────────────────────────────────────────────────────────────┘
```

---

## 6. Choix technologiques

| Technologie | Rôle | Pourquoi ce choix |
|---|---|---|
| Python | Scraping + ETL | Bibliothèques riches : BeautifulSoup, scikit-learn, langdetect |
| Apache Kafka | Streaming | Standard industrie pour les événements temps réel |
| MinIO | Data Lake | Équivalent S3 open-source, déployable en local sans AWS |
| PostgreSQL | Data Warehouse | Robuste, supporte les vues matérialisées, gratuit |
| Apache Airflow | Orchestration | Standard pour les pipelines de données, interface visuelle |
| FastAPI | API REST | Rapide, documentation Swagger auto-générée |
| Chart.js | Graphiques | Léger, pas de framework lourd nécessaire |
| Grafana | Monitoring avancé | Dashboards professionnels connectés directement à PostgreSQL |
| Docker | Déploiement | Reproductible sur n'importe quelle machine en une commande |

---

## 7. Accès aux interfaces

| Interface | URL | Login |
|---|---|---|
| Dashboard principal | http://localhost | — |
| API (Swagger docs) | http://localhost:8000/docs | — |
| Airflow | http://localhost:8080 | admin / admin |
| MinIO | http://localhost:9001 | minioadmin / minioadmin123 |
| Grafana | http://localhost:3000 | admin / admin123 |
| Kafka UI | http://localhost:8090 | — |

---

## 8. Lancement

```bash
# 1. Construire les images Docker
docker compose build

# 2. Démarrer toute la plateforme
docker compose up -d

# 3. Vérifier que tout tourne
docker compose ps

# 4. Voir les logs d'un service
docker compose logs scraper
docker compose logs etl-worker
```
