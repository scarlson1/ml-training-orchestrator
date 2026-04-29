# Deployment

TODO

## Terraform

Terraform is used to manage two deployments: **Oracle VM** and **Cloudflare R2** (S3 storage).

Terraform state is currently managed locally. Uncomment the R2 section in `main.tf` to migrate state to Cloudflare R2.

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

### `cloud-init.sh`

- install required packages: `ca-certificates`, `curl`, `gnupg`, `lsb-release`, `git`, `docker-ce`, `containerd.io`, `docker-buildx-plugin`, `docker-compose-plugin`, etc.
- install & setup `uv`
- run `keepalive` so VM isn't shutdown
- create & enable systemd service for docker compose (`systemctl start` not run until github CI - repo isn't cloned to VM yet)

### Deploy

```bash
# install provider dependencies
terraform init

terraform plan -var-file="terraform.tfvars" -out=tfplan

terraform apply tfplan
```

VM IP and R2 URL are included in outputs to populate github environment variables if not already set. VM uses a consistent IP - don't change when redeployed.

## Github Actions

### `build-images.yml`

Builds `dagster.Dockerfile` and `serving.Dockerfile` → pushes image to registry (`linux/amd64` & `linux/arm64` platforms)

### `deploy.yml`

- Creates `.env` from github environment secrets/variables.
- Wait for [`cloud-init.sh`](/infra/terraform/oracle/cloud-init.sh) to finish installing docker
- authenticate to GHCR to pull images created in `build-images.yml`
- start/restart bmo-compose service on VM
