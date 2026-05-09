#!/bin/bash
# Watch the backfill progress and run learning once it catches up.
# Usage: nohup ./scripts/watch_backfill.sh &

set -e
cd /home/crowdit/crowd-transcription
source venv/bin/activate

TARGET_WATERMARK=224635239  # Today's watermark before backfill
CHECK_INTERVAL=120          # Check every 2 minutes

echo "$(date) Watching backfill progress (target watermark: $TARGET_WATERMARK)..."

while true; do
    CURRENT=$(python3 -c "
from crowdtrans.database import SessionLocal
from crowdtrans.models import Watermark
with SessionLocal() as s:
    wm = s.query(Watermark).filter_by(site_id='karisma').first()
    print(wm.last_dictation_id)
")
    TOTAL=$(python3 -c "
from crowdtrans.database import SessionLocal
from crowdtrans.models import Transcription
with SessionLocal() as s:
    print(s.query(Transcription).count())
")

    PCT=$(python3 -c "print(round(($CURRENT - 222359081) / ($TARGET_WATERMARK - 222359081) * 100, 1))")
    echo "$(date) Watermark: $CURRENT ($PCT%) — $TOTAL transcriptions"

    if [ "$CURRENT" -ge "$TARGET_WATERMARK" ]; then
        echo "$(date) Backfill complete! Running learning..."
        python3 -m crowdtrans.cli learn 2>&1
        echo "$(date) Learning complete. Exiting."
        exit 0
    fi

    sleep $CHECK_INTERVAL
done
