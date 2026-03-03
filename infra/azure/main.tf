# ============================================================
# infra/azure/main.tf
# Creates: Resource Group, Storage Account (ADLS Gen2),
#          and Bronze/Silver/Gold containers
# ============================================================

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
  }

  # Optional but recommended: store state remotely so it's
  # not lost if you wipe your machine. Uncomment once you
  # have a storage account to use as a backend (chicken/egg
  # problem on first apply — leave commented for now).
  #
  # backend "azurerm" {
  #   resource_group_name  = "rg-tfstate"
  #   storage_account_name = "tfstateenergypipeline"
  #   container_name       = "tfstate"
  #   key                  = "azure.terraform.tfstate"
  # }
}

provider "azurerm" {
  features {}
  subscription_id = var.subscription_id
}

# ─────────────────────────────────────────
# LOCALS — computed values used throughout
# ─────────────────────────────────────────
locals {
  # e.g. "energypipeline" + "dev" → "energypipelinedev"
  # Storage account names must be 3-24 chars, lowercase,
  # alphanumeric only — no hyphens allowed
  storage_account_name = "${lower(replace(var.project_name, "-", ""))}${var.environment}"

  # Standard Azure tag block applied to every resource
  common_tags = {
    project     = var.project_name
    environment = var.environment
    managed_by  = "terraform"
  }
}

# ─────────────────────────────────────────
# RESOURCE GROUP
# ─────────────────────────────────────────
resource "azurerm_resource_group" "main" {
  name     = "rg-${var.project_name}-${var.environment}"
  location = var.location
  tags     = local.common_tags
}

# ─────────────────────────────────────────
# STORAGE ACCOUNT (ADLS Gen2)
# is_hns_enabled = true enables the
# hierarchical namespace (Gen2 feature)
# ─────────────────────────────────────────
resource "azurerm_storage_account" "datalake" {
  name                     = local.storage_account_name
  resource_group_name      = azurerm_resource_group.main.name
  location                 = azurerm_resource_group.main.location
  account_tier             = var.storage_account_tier
  account_replication_type = var.storage_replication_type
  account_kind             = "StorageV2"
  is_hns_enabled           = true   # required for ADLS Gen2

  # Enforce HTTPS only
  https_traffic_only_enabled = true
  min_tls_version            = "TLS1_2"

  # Soft delete — recover accidentally deleted files
  # shorter window in dev, longer in prod
  blob_properties {
    delete_retention_policy {
      days = var.soft_delete_retention_days
    }
    container_delete_retention_policy {
      days = var.soft_delete_retention_days
    }
  }

  tags = local.common_tags
}

# ─────────────────────────────────────────
# CONTAINERS — Bronze / Silver / Gold
# Each is a top-level container in the
# storage account (like S3 buckets)
# ─────────────────────────────────────────
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# Quarantine container — bad records from PySpark land here
resource "azurerm_storage_container" "quarantine" {
  name                  = "quarantine"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}
