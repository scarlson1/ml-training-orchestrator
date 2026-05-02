variable "project_name" {
  type    = string
  default = "bmo-pipeline"
}

variable "compartment_id" {
  type        = string
  description = "OCI compartment OCID."
}

variable "region" {
  type        = string
  description = "OCI home region."
}

variable "vm_shape" {
  type    = string
  default = "VM.Standard.A1.Flex"
}

variable "vm_ocpus" {
  type    = number
  default = 1
}

variable "vm_memory_gb" {
  type    = number
  default = 8
}

variable "boot_volume_gb" {
  type    = number
  default = 100
}

variable "ssh_public_key" {
  type        = string
  description = "SSH public key contents to install on the VM."
}