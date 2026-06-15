#!/usr/bin/env bash
# Run the backend against a Kafka broker on another machine, publishing across the LAN.
#
# The stack's broker advertises itself as 'kafka:9092', so a client that bootstraps to the broker's
# IP is redirected to the bare name 'kafka'. Both modes make that name resolve to the remote host:
# docker via the compose overlay's extra_hosts, local via a temporary /etc/hosts entry.
#
# Usage: start-backend.sh --kafka-host <ip> [--mode docker|local] [--kafka-port <port>] [--port <port>]
#   --kafka-host <ip>    Remote machine running the Kafka broker (required)
#   --kafka-port <port>  Broker port (default: KAFKA_PORT from .env, else 9092)
#   --port <port>        Host port to expose the backend on (default: BACKEND_PORT from .env, else 8030)
#   --mode docker|local  docker (default): backend + Prometheus + Grafana as containers on this host.
#                        local: backend as a host JVM, no Prometheus/Grafana.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"
load_env

usage() { sed -n '8,13p' "$0" | sed 's/^# \{0,1\}//'; }

MODE=docker
KAFKA_IP=""
KAFKA_PORT="${KAFKA_PORT:-9092}"
HOST_PORT="${BACKEND_PORT:-8030}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --mode)       MODE="${2:-}"; shift 2 ;;
    --kafka-host) KAFKA_IP="${2:-}"; shift 2 ;;
    --kafka-port) KAFKA_PORT="${2:-}"; shift 2 ;;
    --port)       HOST_PORT="${2:-}"; shift 2 ;;
    -h|--help)    usage; exit 0 ;;
    *)            echo "Unknown argument '$1'" >&2; exit 1 ;;
  esac
done

if [[ -z "$KAFKA_IP" ]]; then
  echo "Missing --kafka-host <ip>. Run with -h for usage." >&2
  exit 1
fi

case "$MODE" in
  docker)
    require_docker
    # KAFKA_HOST is both the broker the backend bootstraps to and the IP the advertised name 'kafka'
    # resolves to via the overlay's extra_hosts.
    export KAFKA_HOST="$KAFKA_IP"
    export KAFKA_PORT BACKEND_PORT="$HOST_PORT"
    export COMPOSE_OVERRIDE="$SCRIPT_DIR/docker-compose.remote-backend.yml"
    # Backend and Prometheus share this host's compose network, so the scrape target is the container port.
    render_prometheus_backend_target "backend:8080"
    echo "→ backend -> kafka ${KAFKA_IP}:${KAFKA_PORT}; starting backend/prometheus/grafana on this host"
    compose up -d --no-deps --build backend prometheus grafana
    compose ps
    ;;
  local)
    ensure_java21
    mvn -q -f "$ROOT_DIR/pom.xml" -pl backend -am package -DskipTests
    # Identifies only the line this script owns, so cleanup never touches a pre-existing 'kafka' entry.
    HOSTS_MARKER="# managed by start-backend.sh"
    cleanup() { sudo sed -i "\|${HOSTS_MARKER}|d" /etc/hosts; }
    sudo sed -i "\|${HOSTS_MARKER}|d" /etc/hosts
    echo "${KAFKA_IP}  kafka  ${HOSTS_MARKER}" | sudo tee -a /etc/hosts >/dev/null
    trap cleanup EXIT
    echo "→ kafka -> ${KAFKA_IP}:${KAFKA_PORT}, backend on http://localhost:${HOST_PORT} (Ctrl-C to stop)"
    SERVER_PORT="$HOST_PORT" KAFKA_HOST_LOCAL=kafka KAFKA_PORT="$KAFKA_PORT" \
      java -jar "$ROOT_DIR/backend/target/backend.jar"
    ;;
  *)
    echo "Unknown --mode '$MODE' (expected docker or local)" >&2
    exit 1
    ;;
esac
