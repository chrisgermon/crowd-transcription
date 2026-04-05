#!/usr/bin/env bash
# Pull latest code from GitHub and restart services.
# Run as root: sudo bash /opt/crowdtrans/deploy/update.sh
set -euo pipefail

REPO_URL="https://github.com/chrisgermon/crowd-transcription.git"
APP_DIR="/opt/crowdtrans"
VENV="$APP_DIR/venv"
BRANCH="main"

echo "$(date '+%Y-%m-%d %H:%M:%S') — Starting CrowdTrans update..."

# If not yet a git repo, clone it (first-time migration from cp-based deploy)
if [ ! -d "$APP_DIR/.git" ]; then
    echo "No git repo found — cloning from $REPO_URL"
    # Back up data and .env before replacing
    cp -a "$APP_DIR/data" /tmp/crowdtrans-data-backup 2>/dev/null || true
    cp "$APP_DIR/.env" /tmp/crowdtrans-env-backup 2>/dev/null || true

    # Clone into a temp dir, then move into place
    rm -rf /tmp/crowdtrans-clone
    git clone --branch "$BRANCH" "$REPO_URL" /tmp/crowdtrans-clone
    # Preserve data dir and .env
    rm -rf /tmp/crowdtrans-clone/data
    rsync -a --exclude='.git' --exclude='data' --exclude='.env' /tmp/crowdtrans-clone/ "$APP_DIR/"
    # Now set up git in the app dir
    cp -a /tmp/crowdtrans-clone/.git "$APP_DIR/.git"
    rm -rf /tmp/crowdtrans-clone

    # Restore data and .env
    cp -a /tmp/crowdtrans-data-backup/* "$APP_DIR/data/" 2>/dev/null || true
    cp /tmp/crowdtrans-env-backup "$APP_DIR/.env" 2>/dev/null || true

    chown -R crowdtrans:crowdtrans "$APP_DIR"
    echo "Git repo initialized at $APP_DIR"
else
    # Pull latest changes
    cd "$APP_DIR"
    sudo -u crowdtrans git fetch origin "$BRANCH"
    sudo -u crowdtrans git reset --hard "origin/$BRANCH"
    echo "Pulled latest from origin/$BRANCH"
fi

# Install/update dependencies
echo "Installing dependencies..."
"$VENV/bin/pip" install --quiet --upgrade pip
"$VENV/bin/pip" install --quiet -r "$APP_DIR/requirements.txt"

# Run database migrations (init_db handles new columns/tables)
echo "Running database migrations..."
sudo -u crowdtrans "$VENV/bin/python" -m crowdtrans.cli init-db

# Update systemd units if changed
cp "$APP_DIR/deploy/crowdtrans-service.service" /etc/systemd/system/
cp "$APP_DIR/deploy/crowdtrans-web.service" /etc/systemd/system/
systemctl daemon-reload

# Restart services
echo "Restarting services..."
systemctl restart crowdtrans-service crowdtrans-web

echo "$(date '+%Y-%m-%d %H:%M:%S') — Update complete."
echo "Service: $(systemctl is-active crowdtrans-service)"
echo "Web:     $(systemctl is-active crowdtrans-web)"
