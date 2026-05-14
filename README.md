# 🗞️ News Platform — Architecture de Données

Plateforme Big Data de collecte et d'analyse d'articles de presse.  
**Stack :** Python · Kafka · MinIO · PostgreSQL · Airflow · Grafana · Docker  
**Compatible :** Mac M3 (ARM64)

---

## 🏗️ Architecture

```
Web Scrapers → Kafka → MinIO (Bronze) → ETL Python → MinIO (Silver/Gold) → PostgreSQL DWH → Grafana
                                                ↑
                                         Apache Airflow (orchestration)
```

## 📦 Services Docker

| Service | Port | Description |
|---|---|---|
| Airflow UI | 8080 | Orchestration des pipelines |
| MinIO Console | 9001 | Data Lake (Bronze/Silver/Gold) |
| Grafana | 3000 | Dashboards analytiques |
| Kafka UI | 8090 | Monitoring des topics |
| PostgreSQL DWH | 5433 | Data Warehouse analytique |
| Redis | 6379 | Cache temps réel |

---

## 🚀 Démarrage rapide (Mac M3)

### Prérequis
```bash
# Vérifier Docker Desktop (ARM64)
docker --version          # >= 24.x
docker compose version    # >= 2.x

# Allouer au minimum dans Docker Desktop :
# RAM : 6 Go | CPU : 4 cœurs | Disk : 20 Go
```

### Installation
```bash
# 1. Cloner le projet
git clone <repo-url>
cd news-platform

# 2. Setup initial
make setup

# 3. Build les images locales
make build

# 4. Démarrer la plateforme
make up

# 5. Vérifier le statut
make status
```

### Accès aux interfaces
```
Airflow   → http://localhost:8080  (admin / admin)
MinIO     → http://localhost:9001  (minioadmin / minioadmin123)
Grafana   → http://localhost:3000  (admin / admin123)
Kafka UI  → http://localhost:8090
```

---

## 📁 Structure du projet

```
news-platform/
├── docker-compose.yml          # Orchestration Docker
├── Makefile                    # Commandes utilitaires
├── .env.example                # Variables d'environnement
│
├── scrapers/                   # Collecte de données
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── main.py                 # Point d'entrée
│   ├── hespress.py             # Scraper Hespress
│   ├── bbc.py                  # Scraper BBC
│   └── rss_producer.py        # Streaming Kafka
│
├── etl/                        # Transformations ETL
│   ├── Dockerfile
│   ├── requirements.txt
│   ├── worker.py               # ETL worker
│   ├── bronze_to_silver.py
│   └── silver_to_gold.py
│
├── dags/                       # Airflow DAGs
│   ├── dag_scraping_batch.py
│   ├── dag_bronze_to_silver.py
│   ├── dag_silver_to_gold.py
│   └── dag_quality_checks.py
│
├── quality/                    # Contrôle qualité
│   └── expectations.py
│
├── warehouse/                  # Requêtes SQL analytiques
│   └── queries.sql
│
└── docker/
    ├── postgres/
    │   └── init.sql            # Schéma DWH
    └── grafana/
        └── provisioning/       # Config auto Grafana
```

---

## 🔧 Commandes utiles

```bash
make up              # Démarrer
make down            # Arrêter
make logs            # Voir les logs
make status          # Statut des containers
make psql-dwh        # Connexion PostgreSQL DWH
make kafka-topics    # Lister les topics Kafka
make refresh-views   # Rafraîchir les vues analytiques
make clean           # Tout supprimer (données incluses)
```

---

## 🏛️ Architecture Médaillon

| Couche | Bucket MinIO | Format | Contenu |
|---|---|---|---|
| Bronze | `bronze/` | JSON | Articles bruts, tels que collectés |
| Silver | `silver/` | Parquet | Nettoyés, normalisés, dédupliqués |
| Gold | `gold/` | Parquet | Agrégés, prêts pour l'analyse |

**Partitionnement Bronze :** `bronze/YYYY/MM/DD/<source>/<hash>.json`

---

## 📊 Qualité des données

Tests automatiques (Great Expectations) :
- ✅ Titre non vide
- ✅ Date valide et non future
- ✅ Contenu > 100 caractères
- ✅ URL unique (déduplication)
- ✅ Langue détectée

---

## ⚠️ Notes Mac M3

Toutes les images Docker sont configurées avec `platform: linux/arm64`.  
Si une image n'est pas disponible en ARM64, Docker Desktop la transcompile automatiquement via Rosetta 2 (plus lent mais fonctionnel).
