# Deployment

## Terraform

### Environment Variables

#### `infra/terraform/terraform.tfvars`

- `project_name`
- `oci_tenancy_ocid`
- `oci_user_ocid`
- `oci_fingerprint`
- `oci_private_key_path`
- `region`
- `ssh_public_key`
- `cloudflare_api_token`
- `cloudflare_account_id`

### Deploy

```bash
# install provider dependencies
terraform init

terraform plan -var-file="terraform.tfvars" -out=tfplan

terraform apply tfplan
```
