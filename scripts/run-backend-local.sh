#!/usr/bin/env bash
# Build the backend and run it on the host (outside Docker), publishing to a Kafka
# broker on another machine.
#
# The stack's broker advertises itself as 'kafka:9092' (docker-compose.yml), so a
# host client that bootstraps to the broker's IP is redirected to the bare name
# 'kafka'. To make that name resolve to the remote machine, this maps 'kafka' ->
# <ip> in /etc/hosts (sudo) for the duration of the run and removes the mapping on
# exit, including on Ctrl-C.
#
# Usage: run-backend-local.sh --kafka-host=<ip> [--kafka-port=<port>] [--port=<port>]
#   --kafka-host=<ip>    Remote machine running the Kafka broker (required)
#   --kafka-port=<port>  Broker port (default: KAFKA_PORT from .env, else 9092)
#   --port=<port>        Local HTTP port (default: BACKEND_PORT from .env, else 8030)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_env

KAFKA_IP=""
KAFKA_PORT="${KAFKA_PORT:-9092}"
SERVER_PORT="${BACKEND_PORT:-8030}"

for arg in "$@"; do
  case "$arg" in
    --kafka-host=*) KAFKA_IP="${arg#*=}" ;;
    --kafka-port=*) KAFKA_PORT="${arg#*=}" ;;
    --port=*)       SERVER_PORT="${arg#*=}" ;;
    -h|--help)      sed -n '11,14p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *)              echo "Unknown argument '$arg'" >&2; exit 1 ;;
  esac
done

if [[ -z "$KAFKA_IP" ]]; then
  echo "Missing --kafka-host=<ip>. Run with -h for usage." >&2
  exit 1
fi

(cd "$ROOT_DIR/backend" && mvn -q package -DskipTests)

# Identifies only the line this script owns, so cleanup never touches a pre-existing
# 'kafka' entry the user maintains for other reasons.
HOSTS_MARKER="# managed by run-backend-local.sh"
cleanup() { sudo sed -i "\|${HOSTS_MARKER}|d" /etc/hosts; }

sudo sed -i "\|${HOSTS_MARKER}|d" /etc/hosts          # clear any leftover from a crashed run
echo "${KAFKA_IP}  kafka  ${HOSTS_MARKER}" | sudo tee -a /etc/hosts >/dev/null
trap cleanup EXIT

echo "→ kafka -> ${KAFKA_IP}:${KAFKA_PORT}, backend on http://localhost:${SERVER_PORT} (Ctrl-C to stop)"
SERVER_PORT="$SERVER_PORT" KAFKA_HOST_LOCAL=kafka KAFKA_PORT="$KAFKA_PORT" \
  java -jar "$ROOT_DIR/backend/target/backend.jar"
