# ============================================================
# infra/azure/secrets.tfvars
# GITIGNORED — never commit this file.
# Contains values that are the same across dev and prod
# but are sensitive.
#
# Apply with both var files together:
#   terraform apply \
#     -var-file="envs/dev.tfvars" \
#     -var-file="secrets.tfvars"
# ============================================================

subscription_id = "661e6042-3f07-403c-8035-39b8cd740872"
