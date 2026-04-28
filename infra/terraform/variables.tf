# ----- Oracle ----- #

variable "project_name" {
  type    = string
  default = "bmo-pipeline"
}

variable "oci_tenancy_ocid" {
    description = "Tenancy OCID. OCI Console → Profile → Tenancy details."
    type        = string
}

variable "oci_user_ocid" {
  description = "User OCID. OCI Console → Profile → User settings."
  type        = string
}

variable "oci_fingerprint" {
  description = "Fingerprint of the OCI API key (shown after upload)."
  type        = string
}

variable "oci_private_key_path" {
  description = "Path to ~/.oci/oci_api_key.pem on your local machine."
  type        = string
  default     = "~/.oci/oci_api_key.pem"
}

variable "region" {
  description = "OCI home region. Always Free quota is tied to this region."
  type        = string
  default     = "us-chicago-1"
}

variable "ssh_public_key" {
  description = "Public key to install on the VM. Contents of ~/.ssh/id_rsa.pub."
  type        = string
}


# ----- Cloudflare ----- #

variable "cloudflare_api_token" {
  description = "Cloudflare API token. Needs 'Workers R2 Storage: Edit' permission."
  type        = string
  sensitive   = true
}

variable "cloudflare_account_id" {
  description = "32-char account ID visible in the R2 overview page URL."
  type        = string
}