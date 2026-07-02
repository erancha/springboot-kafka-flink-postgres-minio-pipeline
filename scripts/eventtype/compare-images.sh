#!/usr/bin/env bash

# Compare image counts in MinIO vs PostgreSQL

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../common.sh
source "$SCRIPT_DIR/../common.sh"

echo "=== Image Count Comparison ==="
echo

echo "Images in MinIO bucket 'images':"
MINIO_COUNT=$(compose exec -T minio mc ls --recursive local/images | wc -l)
echo "  $MINIO_COUNT objects"
echo

echo "Image records in PostgreSQL (processed_events):"
POSTGRES_COUNT=$(compose exec -T postgres psql -U postgres -d warehouse -t -c "SELECT COUNT(*) FROM processed_events WHERE event_type='IMAGE' AND image_object_key IS NOT NULL;")
echo "  $POSTGRES_COUNT records"
echo

echo "Image events aggregated by Flink (event_type_counts_agg):"
FLINK_COUNT=$(compose exec -T postgres psql -U postgres -d warehouse -t -c "SELECT COALESCE(SUM(event_count), 0) FROM event_type_counts_agg WHERE event_type='IMAGE';")
echo "  $FLINK_COUNT events (summed across all 5-minute windows)"
echo

echo "Comparison:"
if [ "$MINIO_COUNT" -eq "$POSTGRES_COUNT" ]; then
  echo "  ✓ MinIO vs PostgreSQL: Counts match!"
else
  DIFF=$((MINIO_COUNT - POSTGRES_COUNT))
  echo "  ✗ MinIO vs PostgreSQL: Mismatch of $DIFF" | awk '{if ($NF < 0) gsub(/of/, "of -"); print}'
fi

echo "  Flink pre-aggregated count: $FLINK_COUNT events"
