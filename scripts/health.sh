#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

require_docker

compose ps

echo

echo "UI:           http://localhost:3030"
echo "Backend API:   http://localhost:8030"
echo "Kafka UI:      http://localhost:8088"
echo "Flink UI:      http://localhost:8081  # port not exposed by default"
echo "MinIO Console: http://localhost:9011"
