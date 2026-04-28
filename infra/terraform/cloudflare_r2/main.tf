terraform {
  required_providers {
    cloudflare = {
      source  = "cloudflare/cloudflare"
      version = "~> 4.0"
    }
  }
}

# The four buckets mirror the local MinIO layout.
# Using for_each so adding a bucket is a one-line change.
locals {
  buckets = toset(["raw", "staging", "rejected", "mlflow-artifacts"])
}

resource "cloudflare_r2_bucket" "bmo" {
  for_each   = local.buckets
  account_id = var.account_id
  name       = "bmo-${each.key}"
  # WNAM = Western North America. Pick the region closest to your Oracle VM.
  # Options: WNAM, ENAM, WEUR, EEUR, APAC
  location   = "ENAM"
}
