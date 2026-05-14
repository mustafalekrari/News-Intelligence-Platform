# ============================================================
#  NEWS PLATFORM — Makefile
#  Usage : make <commande>
# ============================================================

.PHONY: help up down restart logs clean status build

# Couleurs
GREEN  := \033[0;32m
YELLOW := \033[1;33m
CYAN   := \033[0;36m
RESET  := \033[0m

help: ## Affiche l'aide
	@echo "$(CYAN)╔══════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║      NEWS PLATFORM — Commandes           ║$(RESET)"
	@echo "$(CYAN)╚══════════════════════════════════════════╝$(RESET)"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "$(GREEN)  %-20s$(RESET) %s\n", $$1, $$2}'

# ── Démarrage ────────────────────────────────────────────────

setup: ## Première installation (copie .env, crée dossiers)
	@cp -n .env.example .env 2>/dev/null || true
	@mkdir -p dags etl scrapers quality warehouse
	@echo "$(GREEN)✓ Setup terminé. Editez .env si nécessaire.$(RESET)"

build: ## Build les images Docker locales
	@echo "$(YELLOW)→ Build des images...$(RESET)"
	docker compose build --no-cache

up: ## Démarre toute la plateforme
	@echo "$(GREEN)→ Démarrage de la plateforme...$(RESET)"
	docker compose up -d
	@echo ""
	@echo "$(CYAN)╔══════════════════════════════════════════════════╗$(RESET)"
	@echo "$(CYAN)║  Services disponibles :                          ║$(RESET)"
	@echo "$(CYAN)║  Dashboard    → http://localhost                 ║$(RESET)"
	@echo "$(CYAN)║  API          → http://localhost:8000/docs       ║$(RESET)"
	@echo "$(CYAN)║  Airflow UI   → http://localhost:8080            ║$(RESET)"
	@echo "$(CYAN)║  MinIO UI     → http://localhost:9001            ║$(RESET)"
	@echo "$(CYAN)║  Grafana      → http://localhost:3000            ║$(RESET)"
	@echo "$(CYAN)║  Kafka UI     → http://localhost:8090            ║$(RESET)"
	@echo "$(CYAN)╚══════════════════════════════════════════════════╝$(RESET)"

down: ## Arrête tous les services
	@echo "$(YELLOW)→ Arrêt de la plateforme...$(RESET)"
	docker compose down

restart: ## Redémarre tous les services
	docker compose restart

# ── Logs ─────────────────────────────────────────────────────

logs: ## Affiche les logs de tous les services
	docker compose logs -f

logs-scraper: ## Logs du scraper uniquement
	docker compose logs -f scraper

logs-etl: ## Logs de l'ETL worker
	docker compose logs -f etl-worker

logs-airflow: ## Logs Airflow (webserver + scheduler)
	docker compose logs -f airflow-webserver airflow-scheduler

logs-kafka: ## Logs Kafka
	docker compose logs -f kafka

# ── Status ───────────────────────────────────────────────────

status: ## Statut de tous les containers
	@docker compose ps

health: ## Vérifie la santé de chaque service
	@echo "$(CYAN)Vérification des services...$(RESET)"
	@docker compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# ── Nettoyage ─────────────────────────────────────────────────

clean: ## Arrête et supprime les containers + volumes
	@echo "$(YELLOW)⚠️  Suppression de tous les volumes (données perdues!)$(RESET)"
	@read -p "Confirmer ? [y/N] " ans && [ "$$ans" = "y" ]
	docker compose down -v --remove-orphans

clean-soft: ## Arrête sans supprimer les données
	docker compose down --remove-orphans

# ── Accès directs ─────────────────────────────────────────────

psql-dwh: ## Ouvre psql sur le Data Warehouse
	docker exec -it postgres-dwh psql -U news_user -d news_dwh

psql-airflow: ## Ouvre psql sur la DB Airflow
	docker exec -it postgres-airflow psql -U airflow -d airflow

kafka-topics: ## Liste les topics Kafka
	docker exec kafka kafka-topics --bootstrap-server localhost:29092 --list

minio-ls: ## Liste les buckets MinIO
	docker exec minio mc ls local/

# ── Développement ─────────────────────────────────────────────

scraper-test: ## Lance le scraper manuellement
	docker exec scraper python main.py --once

etl-run: ## Lance le pipeline ETL manuellement
	docker exec etl-worker python worker.py --once

refresh-views: ## Rafraîchit les vues matérialisées PostgreSQL
	docker exec postgres-dwh psql -U news_user -d news_dwh \
		-c "SELECT refresh_all_materialized_views();"
