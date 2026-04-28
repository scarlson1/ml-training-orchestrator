output "oracle_vm_public_ip" {
  description = "Public IP of the Oracle control-plane VM."
  value       = module.oracle.vm_public_ip
}

output "oracle_vm_ssh" {
  description = "SSH command to connect."
  value       = "ssh ubuntu@${module.oracle.vm_public_ip}"
}

output "r2_bucket_names" {
  description = "Map of logical name → R2 bucket name."
  value       = module.cloudflare_r2.bucket_names
}

output "r2_endpoint_url" {
  description = "S3-compatible endpoint for boto3/duckdb/MLflow."
  value       = module.cloudflare_r2.endpoint_url
}

# Printed after every apply — tells you exactly what to do next
output "next_steps" {
  value = <<-EOT
    ✓ Terraform complete. Manual steps remaining:

    1. Generate R2 access keys (Terraform cannot do this):
       Cloudflare dashboard → R2 → Manage R2 API Tokens
       → Create token with "Object Read & Write" on all buckets
       → Save the Access Key ID + Secret Access Key

    2. SSH to the control plane and deploy:
       ssh ubuntu@${module.oracle.vm_public_ip}

    3. Set Fly.io secrets and deploy the serving API:
       fly secrets set MLFLOW_TRACKING_URI=http://${module.oracle.vm_public_ip}:5000 \
                       REDIS_URL=<upstash-url> \
                       AWS_ENDPOINT_URL=https://${var.cloudflare_account_id}.r2.cloudflarestorage.com \
                       AWS_ACCESS_KEY_ID=<r2-key-id> \
                       AWS_SECRET_ACCESS_KEY=<r2-secret>
       fly deploy --config fly.toml
  EOT
}