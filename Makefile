# ============================================================
# Makefile — convenience commands for the pipeline
# Usage: make <target>
# ============================================================

.PHONY: help up down logs ps \
        tf-init tf-plan-dev tf-apply-dev tf-plan-prod tf-apply-prod \
        dbt-run dbt-test dbt-docs \
        test lint \
        tunnel-db

# ── Default target ───────────────────────────────────────────
help:
	@echo ""
	@echo "  Local dev"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make up              Start all services (docker compose)"
	@echo "  make down            Stop all services"
	@echo "  make logs            Tail logs for all services"
	@echo "  make ps              Show running containers"
	@echo ""
	@echo "  Terraform"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make tf-init         Init all terraform workspaces"
	@echo "  make tf-plan-dev     Plan azure dev environment"
	@echo "  make tf-apply-dev    Apply azure dev environment"
	@echo "  make tf-plan-prod    Plan azure prod environment"
	@echo "  make tf-apply-prod   Apply azure prod environment"
	@echo ""
	@echo "  dbt"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make dbt-run         Run all dbt models"
	@echo "  make dbt-test        Run all dbt tests"
	@echo "  make dbt-docs        Generate + serve dbt docs"
	@echo ""
	@echo "  Quality"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make test            Run all python tests"
	@echo "  make lint            Run ruff linter"
	@echo ""
	@echo "  Production"
	@echo "  ─────────────────────────────────────────────────"
	@echo "  make tunnel-db       SSH tunnel to prod postgres"
	@echo ""

# ── Local dev ────────────────────────────────────────────────
up:
	docker compose up -d
	@echo "Services started:"
	@echo "  Airflow:   http://localhost:8080  (admin/admin)"
	@echo "  Kafka UI:  http://localhost:8090"
	@echo "  Superset:  http://localhost:8088"
	@echo "  Postgres:  localhost:5432"

down:
	docker compose down

logs:
	docker compose logs -f

ps:
	docker compose ps

# ── Terraform ────────────────────────────────────────────────
tf-init:
	cd infra/azure   && terraform init
	cd infra/services && terraform init

tf-plan-dev:
	cd infra/azure && terraform plan \
		-var-file="envs/dev.tfvars" \
		-var-file="secrets.tfvars"

tf-apply-dev:
	cd infra/azure && terraform apply \
		-var-file="envs/dev.tfvars" \
		-var-file="secrets.tfvars"
	@echo "Dev storage deployed. Run 'make tf-output-dev' to get .env values."

tf-plan-prod:
	cd infra/azure && terraform plan \
		-var-file="envs/prod.tfvars" \
		-var-file="secrets.tfvars"

tf-apply-prod:
	cd infra/azure && terraform apply \
		-var-file="envs/prod.tfvars" \
		-var-file="secrets.tfvars"

tf-output-dev:
	@cd infra/azure && terraform output -raw env_file_snippet

# ── dbt ──────────────────────────────────────────────────────
dbt-run:
	cd transform/energy_pipeline && set -a && . ../../.env && set +a && dbt run --profiles-dir .. --threads 4

dbt-test:
	cd transform/energy_pipeline && set -a && . ../.env && set +a && dbt test --profiles-dir ..

dbt-docs:
	cd transform/energy_pipeline && set -a && . ../.env && set +a && dbt docs generate --profiles-dir . && dbt docs serve --profiles-dir ..

# ── Quality ──────────────────────────────────────────────────
test:
	pytest ingestion/tests/ processing/tests/ -v

lint:
	ruff check ingestion/ processing/ orchestration/

# ── Production ───────────────────────────────────────────────
# Opens an SSH tunnel so you can connect to prod postgres
# at localhost:5433 without exposing the port publicly
tunnel-db:
	@echo "Tunnelling prod postgres to localhost:5433..."
	@echo "Connect with: psql -h localhost -p 5433 -U airflow -d airflow"
	ssh -L 5433:localhost:5432 $(VPS_USER)@$(VPS_IP) -N