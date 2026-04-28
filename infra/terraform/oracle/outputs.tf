# output "vm_public_ip" {
#   description = "Public IP of the control-plane VM."
#   value       = oci_core_instance.control_plane.public_ip
# }

output "vm_id" {
  description = "OCID of the VM instance."
  value       = oci_core_instance.control_plane.id
}

output "vm_public_ip" {
  description = "Public IP of the control-plane VM."
  value = oci_core_public_ip.control_plane.ip_address
}