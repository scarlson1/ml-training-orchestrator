output "bucket_names" {
  description = "Map of logical name → actual R2 bucket name."
  value       = { for k, v in cloudflare_r2_bucket.bmo : k => v.name }
}

output "endpoint_url" {
  description = "S3-compatible endpoint URL for boto3, DuckDB, and MLflow."
  value       = "https://${var.account_id}.r2.cloudflarestorage.com"
}
