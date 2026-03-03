# ============================================================
# infra/azure/outputs.tf
# These values are referenced by infra/services/ via
# terraform_remote_state, and printed after apply so
# you can paste them into your .env
# ============================================================

output "resource_group_name" {
  description = "Name of the created resource group"
  value       = azurerm_resource_group.main.name
}

output "storage_account_name" {
  description = "Storage account name — use as AZURE_STORAGE_ACCOUNT_NAME in .env"
  value       = azurerm_storage_account.datalake.name
}

output "storage_account_key" {
  description = "Primary access key — use as AZURE_STORAGE_ACCOUNT_KEY in .env"
  value       = azurerm_storage_account.datalake.primary_access_key
  sensitive   = true   # won't print in logs, but accessible via output -json
}

output "storage_account_connection_string" {
  description = "Full connection string — useful for local tooling"
  value       = azurerm_storage_account.datalake.primary_connection_string
  sensitive   = true
}

output "bronze_container_name" {
  value = azurerm_storage_container.bronze.name
}

output "silver_container_name" {
  value = azurerm_storage_container.silver.name
}

output "gold_container_name" {
  value = azurerm_storage_container.gold.name
}

output "quarantine_container_name" {
  value = azurerm_storage_container.quarantine.name
}

# Convenience output — prints the exact lines to add to .env
output "env_file_snippet" {
  description = "Copy-paste this block into your .env file"
  sensitive   = true
  value       = <<-EOT
    AZURE_STORAGE_ACCOUNT_NAME=${azurerm_storage_account.datalake.name}
    AZURE_STORAGE_ACCOUNT_KEY=${azurerm_storage_account.datalake.primary_access_key}
  EOT
}
