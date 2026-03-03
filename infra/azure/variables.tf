# ============================================================
# infra/azure/variables.tf
# ============================================================

variable "subscription_id" {
  description = "Azure subscription ID"
  type        = string
  sensitive   = true
}

variable "project_name" {
  description = "Project name, used in resource naming. Lowercase, no spaces."
  type        = string
  default     = "energypipeline"

  validation {
    condition     = can(regex("^[a-z0-9]+$", var.project_name))
    error_message = "project_name must be lowercase alphanumeric only (no hyphens — storage account naming restriction)."
  }
}

variable "environment" {
  description = "Deployment environment"
  type        = string

  validation {
    condition     = contains(["dev", "prod"], var.environment)
    error_message = "environment must be either 'dev' or 'prod'."
  }
}

variable "location" {
  description = "Azure region to deploy resources into"
  type        = string
  default     = "westeurope"
}

variable "storage_account_tier" {
  description = "Storage account performance tier"
  type        = string
  default     = "Standard"

  validation {
    condition     = contains(["Standard", "Premium"], var.storage_account_tier)
    error_message = "Must be Standard or Premium."
  }
}

variable "storage_replication_type" {
  description = "Storage replication strategy. LRS = cheapest (dev). GRS = geo-redundant (prod)."
  type        = string

  validation {
    condition     = contains(["LRS", "GRS", "RAGRS", "ZRS"], var.storage_replication_type)
    error_message = "Must be one of: LRS, GRS, RAGRS, ZRS."
  }
}

variable "soft_delete_retention_days" {
  description = "Number of days deleted blobs are recoverable. Shorter in dev, longer in prod."
  type        = number

  validation {
    condition     = var.soft_delete_retention_days >= 1 && var.soft_delete_retention_days <= 365
    error_message = "Must be between 1 and 365 days."
  }
}
