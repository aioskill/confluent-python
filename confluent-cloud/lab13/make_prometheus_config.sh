#!/usr/bin/env bash
# Write the Prometheus configuration for Lab 13 into this directory:
#   prometheus.yml    scrape job for the Confluent Cloud Metrics API export endpoint
#   cloud_api_secret  the Cloud API secret (mode 600), read by Prometheus as password_file
#
# The Kafka cluster ID, the Cloud API key and secret come from
# ./confluent-cloud/.env (BOOTSTRAP_SERVERS, CLOUD_API_KEY,
# CLOUD_API_SECRET); the Flink compute pool comes from FLINK_COMPUTE_POOL_ID when it is set.
# Pass cluster IDs as arguments to scrape several Kafka clusters.
#
# Usage:  ./make_prometheus_config.sh [--schema-registry lsrc-...] [--compute-pool lfcp-...] [KAFKA_CLUSTER_ID ...]
set -euo pipefail

HERE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
ENV_FILE="$HERE/../.env"
[[ -f "$ENV_FILE" ]] || { echo "Error: $ENV_FILE not found." >&2; exit 1; }

get() { sed -n "s/^$1=//p" "$ENV_FILE" | head -1; }
KEY=$(get CLOUD_API_KEY)
SECRET=$(get CLOUD_API_SECRET)
[[ -n "$KEY" && -n "$SECRET" ]] || { echo "Error: set CLOUD_API_KEY and CLOUD_API_SECRET in $ENV_FILE." >&2; exit 1; }

SR_ID=""
POOL_ID=$(get FLINK_COMPUTE_POOL_ID)
while [[ $# -gt 0 ]]; do
  case "$1" in
    --schema-registry) SR_ID=$2; shift 2 ;;
    --compute-pool) POOL_ID=$2; shift 2 ;;
    *) break ;;
  esac
done

if [[ $# -gt 0 ]]; then
  CLUSTERS=("$@")
else
  BOOTSTRAP=$(get BOOTSTRAP_SERVERS)
  CLUSTERS=("${BOOTSTRAP%%.*}")   # lkc-xxxxx.region.provider.confluent.cloud:9092 -> lkc-xxxxx
fi
[[ "${CLUSTERS[0]}" =~ ^lkc- ]] || { echo "Error: '${CLUSTERS[0]}' is not a Kafka cluster ID (lkc-...)." >&2; exit 1; }

umask 077
printf '%s' "$SECRET" > "$HERE/cloud_api_secret"

{
  cat <<YAML
global:
  scrape_interval: 1m
  scrape_timeout: 1m

rule_files:
  - /etc/prometheus/alerts.yml

scrape_configs:
  - job_name: confluent-cloud
    scheme: https
    static_configs:
      - targets: ["api.telemetry.confluent.cloud"]
    metrics_path: /v2/metrics/cloud/export
    basic_auth:
      username: $KEY
      password_file: /etc/prometheus/cloud_api_secret
    honor_timestamps: true
    params:
      "resource.kafka.id":
YAML
  for id in "${CLUSTERS[@]}"; do echo "        - $id"; done
  if [[ -n "$SR_ID" ]]; then
    echo '      "resource.schema_registry.id":'; echo "        - $SR_ID"
  fi
  if [[ -n "$POOL_ID" ]]; then
    echo '      "resource.compute_pool.id":'; echo "        - $POOL_ID"
  fi
} > "$HERE/prometheus.yml"

echo "Wrote $HERE/prometheus.yml for Kafka: ${CLUSTERS[*]}${SR_ID:+, Schema Registry: $SR_ID}${POOL_ID:+, compute pool: $POOL_ID}"
echo "Wrote $HERE/cloud_api_secret (mode 600; do not commit it)"
