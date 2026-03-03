# ============================================================
# infra/azure/envs/prod.tfvars
# Apply with:
#   terraform apply -var-file="envs/prod.tfvars"
# ============================================================

environment                = "prod"
location                   = "westeurope"
storage_account_tier       = "Standard"
storage_replication_type   = "GRS"       # geo-redundant — data replicated to secondary region
soft_delete_retention_days = 30          # longer recovery window in prod
