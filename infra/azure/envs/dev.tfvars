# ============================================================
# infra/azure/envs/dev.tfvars
# Apply with:
#   terraform apply -var-file="envs/dev.tfvars"
# ============================================================

environment                = "dev"
location                   = "westeurope"
storage_account_tier       = "Standard"
storage_replication_type   = "LRS"       # cheapest — no geo-redundancy needed in dev
soft_delete_retention_days = 7
