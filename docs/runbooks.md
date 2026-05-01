# Runbooks

## Backfilling a Historical Partition

### When to Use

### Step-by-Step

### Verifying the Backfill

## Hot-Swapping the Production Model

### When to Use

### Step-by-Step (MLflow Registry → `/admin/reload`)

### Rollback Procedure

## Re-Materializing Online Features

### When to Use

### Step-by-Step

### Verifying Redis Is Up-to-Date

## Debugging a Failed Dagster Run

### Reading the Run Log

### Common Failure Points by Phase

### Re-Running a Specific Asset

## Investigating Data Leakage

### Symptoms

### Running the Planted-Value Leakage Test

### Tracing Leakage to Its Source

## Recovering from a Schema Migration

### Iceberg Schema Evolution Commands

### dbt Model Recompile

### Feast `feast apply` After Schema Change

## Rotating Secrets / Credentials

### Updating GitHub Secrets

### Propagating to Oracle VM

### Updating `.env.prod`

## Rebuilding the Dagster Code Server

### When to Rebuild the Docker Image

### Restarting the Code Server Without Full Stack Restart

## Inspecting Iceberg Table State

### Querying via DuckDB

### Listing Snapshots

### Rolling Back to a Previous Snapshot

## VM Becomes Unresponsive During a Training Run

### Signs

- Cursor SSH drops mid-session with `Socket closed without exit code`
- Subsequent SSH attempts time out for 10–30 minutes
- VM eventually recovers on its own; Dagster run shows as failed or still running

### Cause

The Oracle ARM VM (1 OCPU / 6 GB RAM) has no swap by default. When DuckDB runs the
`mart_training_dataset` weather PIT joins it can exhaust available RAM, causing the
kernel to thrash trying to free pages. The VM becomes too busy to service SSH connections
until the OOM killer terminates the offending process.

### Confirm It Was OOM

After regaining SSH access:

```bash
dmesg | grep -i "oom\|killed process" | tail -20
free -h
```

### Add Swap (one-time fix, run on the VM)

```bash
# Create a 4 GB swap file on the boot volume
sudo fallocate -l 4G /swapfile

# Lock down permissions — kernel requires this before activating swap
sudo chmod 600 /swapfile

# Format the file as a swap area
sudo mkswap /swapfile

# Activate immediately for this session
sudo swapon /swapfile

# Persist across reboots
echo '/swapfile none swap sw 0 0' | sudo tee -a /etc/fstab
```

Verify with `free -h` — you should see 4 GB under the Swap row.

### Terminate the Stuck Run

Go to the Dagster UI → Runs → find the stuck run → click **Terminate**.
If the UI terminate button doesn't work, update the run status directly in Postgres:

```sql
UPDATE runs SET status = 'FAILURE'
WHERE status = 'STARTED'
  AND run_id = '<run-id>';
```

### Long-Term Fix

The Oracle A1.Flex free tier allows up to 4 OCPUs / 24 GB. Bumping to 2 OCPUs / 12 GB
eliminates the memory pressure entirely. Update `infra/terraform/oracle/variables.tf`:

```hcl
variable "vm_ocpus"     { default = 2 }
variable "vm_memory_gb" { default = 12 }
```

Then run `terraform apply` from `infra/terraform/oracle/`.

## Disaster Recovery

### Full Stack Rebuild from Scratch

### Restoring from R2 Backups
