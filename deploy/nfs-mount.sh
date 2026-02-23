#!/usr/bin/env bash
set -euo pipefail

NFS_SERVER="unitedpdcor01"
NFS_EXPORT="/visage/local/visage/share/repository/dictation-audio"
MOUNT_POINT="/mnt/visage-audio"

echo "=== Configuring NFS mount ==="

mkdir -p "$MOUNT_POINT"

# Add to fstab if not already present
FSTAB_ENTRY="${NFS_SERVER}:${NFS_EXPORT} ${MOUNT_POINT} nfs ro,soft,timeo=30,retrans=3 0 0"
if ! grep -qF "$MOUNT_POINT" /etc/fstab; then
    echo "$FSTAB_ENTRY" >> /etc/fstab
    echo "Added fstab entry"
fi

# Mount now
if ! mountpoint -q "$MOUNT_POINT"; then
    mount "$MOUNT_POINT"
    echo "Mounted $NFS_SERVER:$NFS_EXPORT -> $MOUNT_POINT"
else
    echo "Already mounted: $MOUNT_POINT"
fi

# Verify
ls "$MOUNT_POINT" > /dev/null && echo "NFS mount accessible" || echo "WARNING: NFS mount not accessible"
