#!/usr/bin/env bash
set -euo pipefail

echo "=== CrowdTrans VM Setup ==="

# System packages
apt-get update
apt-get install -y python3.11 python3.11-venv python3-pip nfs-common freetds-dev

# Create service user
if ! id crowdtrans &>/dev/null; then
    useradd --system --shell /usr/sbin/nologin --home-dir /opt/crowdtrans crowdtrans
    echo "Created crowdtrans user"
fi

# Application directory
mkdir -p /opt/crowdtrans/data
chown -R crowdtrans:crowdtrans /opt/crowdtrans

# Copy application code
cp -r /tmp/crowdtrans-deploy/* /opt/crowdtrans/
chown -R crowdtrans:crowdtrans /opt/crowdtrans

# Python virtual environment
if [ ! -d /opt/crowdtrans/venv ]; then
    python3.11 -m venv /opt/crowdtrans/venv
fi
/opt/crowdtrans/venv/bin/pip install --upgrade pip
/opt/crowdtrans/venv/bin/pip install -r /opt/crowdtrans/requirements.txt

# .env file
if [ ! -f /opt/crowdtrans/.env ]; then
    cp /opt/crowdtrans/.env.example /opt/crowdtrans/.env
    echo "Created .env from .env.example — edit with actual values"
fi
chown crowdtrans:crowdtrans /opt/crowdtrans/.env
chmod 600 /opt/crowdtrans/.env

# NFS mount for Visage audio (skip if Visage disabled)
if grep -q "VISAGE_ENABLED=true" /opt/crowdtrans/.env 2>/dev/null; then
    bash /opt/crowdtrans/deploy/nfs-mount.sh
fi

# Initialize database
sudo -u crowdtrans /opt/crowdtrans/venv/bin/python -m crowdtrans.cli init-db

# Install systemd units
cp /opt/crowdtrans/deploy/crowdtrans-service.service /etc/systemd/system/
cp /opt/crowdtrans/deploy/crowdtrans-web.service /etc/systemd/system/
systemctl daemon-reload

systemctl enable crowdtrans-service crowdtrans-web
systemctl start crowdtrans-service crowdtrans-web

echo "=== Setup complete ==="
echo "Web UI: http://$(hostname -I | awk '{print $1}'):8080"
echo "Service logs: journalctl -u crowdtrans-service -f"
echo "Web logs:     journalctl -u crowdtrans-web -f"
echo ""
echo "Sites configured:"
sudo -u crowdtrans /opt/crowdtrans/venv/bin/python -m crowdtrans.cli sites
