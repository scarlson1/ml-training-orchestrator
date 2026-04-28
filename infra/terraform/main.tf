terraform {
    required_version = ">= 1.8"

    required_providers {
      oci = {
        source = "oracle/oci"
        version = "~> 6.0"
      }
      cloudflare = {
        source = "cloudflare/cloudflare"
        version = "~> 4.0"
      }
      tls = {
        source = "hashicorp/tls"
        version = "~> 4.0"
      }
      local = {
        source = "hashicorp/local"
        version = "~> 2.0"
      }
    }

    # Optional: store state in R2 after buckets exist.
    # Uncomment after first `terraform apply`, then `terraform init -migrate-state`.
    # backend "s3" {
    #   bucket                      = "bmo-terraform-state"
    #   key                         = "terraform.tfstate"
    #   region                      = "auto"
    #   skip_credentials_validation = true
    #   skip_metadata_api_check     = true
    #   skip_region_validation      = true
    #   force_path_style            = true
    #   endpoints = {
    #     s3 = "https://{vars.cloudflare_account_id}.r2.cloudflarestorage.com"
    #   }
    # }
}

provider "oci" {
    tenancy_ocid    = var.oci_tenancy_ocid
    user_ocid       = var.oci_user_ocid
    fingerprint     = var.oci_fingerprint
    private_key_path = var.oci_private_key_path
    region          = var.region
}

provider "cloudflare" {
    api_token = var.cloudflare_api_token
}

# Compartment created first — oracle module references its ID,
# so Terraform automatically provisions this before any oracle resources.
resource "oci_identity_compartment" "bmo" {
  compartment_id = var.oci_tenancy_ocid
  name           = "bmo"
  description    = "BMO ML training orchestrator"

  lifecycle {
    prevent_destroy = true
  }
}

module "oracle" {
  source = "./oracle"

  compartment_id = oci_identity_compartment.bmo.id
  region         = var.region
  ssh_public_key = var.ssh_public_key
  project_name   = var.project_name
}

module "cloudflare_r2" {
  source = "./cloudflare_r2"

  account_id = var.cloudflare_account_id
}
