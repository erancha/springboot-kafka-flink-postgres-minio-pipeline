#!/usr/bin/env bash

# Compare image counts in MinIO vs PostgreSQL

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

echo "=== Image Count Comparison ==="
echo

# Count images in MinIO using the mc CLI from the minio container
echo "Images in MinIO bucket 'images':"
MINIO_COUNT=$(compose exec -T minio mc ls --recursive local/images | wc -l)
echo "  $MINIO_COUNT objects"
echo

# Count images in PostgreSQL (processed_events with eventType='IMAGE' and non-null image_object_key)
echo "Image records in PostgreSQL (processed_events):"
POSTGRES_COUNT=$(compose exec -T postgres psql -U postgres -d warehouse -t -c "SELECT COUNT(*) FROM processed_events WHERE event_type='IMAGE' AND image_object_key IS NOT NULL;")
echo "  $POSTGRES_COUNT records"
echo

# Count images aggregated by Flink in real-time (event_type_counts_agg)
echo "Image events aggregated by Flink (event_type_counts_agg):"
FLINK_COUNT=$(compose exec -T postgres psql -U postgres -d warehouse -t -c "SELECT COALESCE(SUM(event_count), 0) FROM event_type_counts_agg WHERE event_type='IMAGE';")
echo "  $FLINK_COUNT events (summed across all 5-minute windows)"
echo

# Compare
echo "Comparison:"
if [ "$MINIO_COUNT" -eq "$POSTGRES_COUNT" ]; then
  echo "  ✓ MinIO vs PostgreSQL: Counts match!"
else
  DIFF=$((MINIO_COUNT - POSTGRES_COUNT))
  echo "  ✗ MinIO vs PostgreSQL: Mismatch of $DIFF" | awk '{if ($NF < 0) gsub(/of/, "of -"); print}'
fi

echo "  Flink pre-aggregated count: $FLINK_COUNT events"
