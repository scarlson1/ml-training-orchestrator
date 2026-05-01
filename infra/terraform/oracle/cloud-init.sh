#!/bin/bash
# Runs once on first boot as root (via cloud-init).
# After this completes, the VM is ready for: git clone + docker compose up
set -euo pipefail

# Ubuntu starts unattended-upgrades immediately on boot; wait for the apt lock
# before touching apt or the first apt-get update exits 100 and aborts this script.
systemctl stop unattended-upgrades || true
while fuser /var/lib/dpkg/lock-frontend /var/lib/apt/lists/lock >/dev/null 2>&1; do
  sleep 2
done

# ── System update ─────────────────────────────────────────────────────────────
apt-get update -q
apt-get upgrade -y -q

# ── Docker (official apt repo, not the distro package) ───────────────────────
# The ubuntu apt repo ships an outdated Docker version. The official repo
# gives us docker-compose-plugin (v2) and buildx.
apt-get install -y ca-certificates curl gnupg lsb-release git

install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
  | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg

echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
  https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
  > /etc/apt/sources.list.d/docker.list

apt-get update -q
apt-get install -y \
  docker-ce docker-ce-cli containerd.io \
  docker-buildx-plugin docker-compose-plugin

systemctl enable --now docker
usermod -aG docker ubuntu   # lets ubuntu user run docker without sudo

# ── uv ────────────────────────────────────────────────────────────────────────
# Needed to run bmo Python code directly on the VM (feast apply, dbt parse, etc.)
curl -LsSf https://astral.sh/uv/install.sh | HOME=/home/ubuntu sh
chown -R ubuntu:ubuntu /home/ubuntu/.local

# ── Swap ──────────────────────────────────────────────────────────────────────
# No swap by default on Oracle ARM. Without it, DuckDB training jobs exhaust
# RAM and the kernel thrashes until the OOM killer fires, making the VM
# unresponsive for minutes at a time.
fallocate -l 4G /swapfile
chmod 600 /swapfile
mkswap /swapfile
swapon /swapfile
echo '/swapfile none swap sw 0 0' >> /etc/fstab

# ── VM keepalive cron ─────────────────────────────────────────────────────────
# Oracle reclaims idle Always Free VMs after ~7 days of "inactivity".
# This pings the Dagster health endpoint every 30 minutes to prevent that.
cat > /etc/cron.d/vm-keepalive <<'EOF'
*/30 * * * * ubuntu curl -sf http://localhost:3000/server_info > /dev/null 2>&1 || true
EOF
chmod 644 /etc/cron.d/vm-keepalive


# ── systemd service for docker compose ───────────────────────────────────────
# cloud-init writes the unit file; systemd manages the compose lifecycle
# from this point on. On every boot: pull latest images, then start.

cat > /etc/systemd/system/bmo-compose.service <<'EOF'
[Unit]
Description=BMO Control Plane (docker compose)
Documentation=https://github.com/youruser/ml-training-orchestrator
Requires=docker.service
After=docker.service network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ubuntu
WorkingDirectory=/home/ubuntu/ml-training-orchestrator
EnvironmentFile=/home/ubuntu/ml-training-orchestrator/.env

# Pull before start so deploys via `systemctl restart` pick up new images
ExecStartPre=/usr/bin/docker compose -f infra/compose/compose.prod.yml pull --quiet
ExecStart=/usr/bin/docker compose -f infra/compose/compose.prod.yml up --remove-orphans
ExecStop=/usr/bin/docker compose -f infra/compose/compose.prod.yml down

# Restart automatically if compose crashes (not on clean `systemctl stop`)
Restart=on-failure
RestartSec=30s

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable bmo-compose
# Don't `systemctl start` here — the repo isn't cloned yet at cloud-init time.
# The GitHub Actions deploy workflow starts it on first deploy.
