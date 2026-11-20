#!/usr/bin/env bash
# Prepare the shared service account of a class (Labs 4, 6A, 6B, 8, 11, 13).
#
# Usage:
#   ./prepare_common_lab.sh <ENVIRONMENT_ID> <KAFKA_CLUSTER_ID> <SERVICE_ACCOUNT_ID> [<FLINK_COMPUTE_POOL_ID>]
#   e.g. ./prepare_common_lab.sh env-qqoypp lkc-6k6xqxj sa-5m31kw8 lfcp-9kyq5py
#
# For the given environment, cluster and service account it
#   1. gives the service account full access to the Kafka cluster (Kafka ACLs on all
#      topics, all consumer groups, all transactional IDs and the cluster itself),
#   2. creates a Kafka API key for the service account,
#   3. makes sure Schema Registry exists for the environment (it is provisioned together
#      with the first Kafka cluster of the environment, so this only waits for it),
#   4. gives the service account ResourceOwner on all Schema Registry subjects, and
#      creates a Schema Registry API key for it,
#   5. gives the service account the FlinkDeveloper role on the environment, so that it can
#      create and run Flink SQL statements (Lab 11),
#      DeveloperRead and DeveloperWrite on all topics of the cluster (Flink needs the RBAC
#      roles, ACLs are not enough; Lab 11), and MetricsViewer on the organization (the
#      Metrics API, Lab 13), and creates a Cloud API key for the Metrics API (Lab 13),
#   6. prints the endpoints and keys, and writes them to confluent-cloud/.env (next to
#      this script; created if missing): BOOTSTRAP_SERVERS, REST_ENDPOINT, KEY, SECRET,
#      SCHEMA_REGISTRY_ENDPOINT, SCHEMA_REGISTRY_KEY, SCHEMA_REGISTRY_SECRET. Other lines
#      of .env (for example ROOT_USERNAME) are kept. The secrets are shown only once.
#      With a Flink compute pool ID it also writes FLINK_COMPUTE_POOL_ID to .env (Lab 13
#      scrapes the pool's metrics).
#
# Safe to run again for the permissions (existing ACLs and role bindings are left as
# they are), but every run creates NEW API keys and replaces the ones in .env.
# Requires the Confluent CLI, logged in as an administrator, and jq.

set -euo pipefail

usage() { sed -n '2,30p' "$0" >&2; exit 2; }
[[ $# -eq 3 || $# -eq 4 ]] || usage
ENV_ID=$1
CLUSTER_ID=$2
SA_ID=$3
POOL_ID=${4:-}

[[ "$ENV_ID" =~ ^env- ]]  || { echo "Error: '$ENV_ID' is not an environment ID (env-...)." >&2; exit 2; }
[[ "$CLUSTER_ID" =~ ^lkc- ]] || { echo "Error: '$CLUSTER_ID' is not a Kafka cluster ID (lkc-...)." >&2; exit 2; }
[[ "$SA_ID" =~ ^sa- ]]    || { echo "Error: '$SA_ID' is not a service account ID (sa-...)." >&2; exit 2; }
[[ -z "$POOL_ID" || "$POOL_ID" =~ ^lfcp- ]] || { echo "Error: '$POOL_ID' is not a Flink compute pool ID (lfcp-...)." >&2; exit 2; }
for tool in confluent jq; do
  command -v "$tool" >/dev/null || { echo "Error: $tool was not found in PATH." >&2; exit 1; }
done

step() { echo "==> $*" >&2; }

# "acl create" fails when the identical ACL already exists, which is fine on a rerun.
acl() {
  confluent kafka acl create --allow --service-account "$SA_ID" \
    --environment "$ENV_ID" --cluster "$CLUSTER_ID" "$@" >/dev/null 2>&1 || {
      confluent kafka acl list --service-account "$SA_ID" --environment "$ENV_ID" --cluster "$CLUSTER_ID" \
        >/dev/null 2>&1 || { echo "Error: cannot create ACLs on $CLUSTER_ID (is the CLI logged in?)." >&2; exit 1; }
    }
}

step "Checking the cluster $CLUSTER_ID and the service account $SA_ID"
cluster_json=$(confluent kafka cluster describe "$CLUSTER_ID" --environment "$ENV_ID" -o json) \
  || { echo "Error: cluster $CLUSTER_ID not found in $ENV_ID (log in with 'confluent login --save')." >&2; exit 1; }
confluent iam service-account describe "$SA_ID" >/dev/null \
  || { echo "Error: service account $SA_ID not found." >&2; exit 1; }

step "Granting full Kafka access to $SA_ID (ACLs)"
topic_ops=read,write,create,delete,alter,describe,describe-configs,alter-configs
acl --operations "$topic_ops" --topic '*'
acl --operations read,delete,describe --consumer-group '*'
acl --operations write,describe --transactional-id '*'
acl --operations create,alter,describe,cluster-action,describe-configs,alter-configs,idempotent-write --cluster-scope

step "Creating a Kafka API key"
kafka_key_json=$(confluent api-key create --resource "$CLUSTER_ID" --service-account "$SA_ID" \
  --environment "$ENV_ID" --description "common lab key for $SA_ID" -o json)

step "Looking for Schema Registry in $ENV_ID"
sr_json=''
for _ in $(seq 60); do
  if sr_json=$(confluent schema-registry cluster describe --environment "$ENV_ID" -o json 2>/dev/null); then
    break
  fi
  sr_json=''
  sleep 5
done
[[ -n "$sr_json" ]] || {
  echo "Error: Schema Registry is not enabled in $ENV_ID after 5 minutes. It is created with the first" >&2
  echo "       Kafka cluster of the environment; check the Stream Governance package of the environment." >&2
  exit 1
}
sr_id=$(jq -r '.cluster' <<<"$sr_json")

step "Granting $SA_ID ResourceOwner on all subjects of $sr_id"
confluent iam rbac role-binding create --principal "User:$SA_ID" --role ResourceOwner \
  --environment "$ENV_ID" --schema-registry-cluster "$sr_id" --resource 'Subject:*' >/dev/null 2>&1 \
  || echo "    (role binding already exists or could not be created; check it with 'confluent iam rbac role-binding list')" >&2

step "Granting $SA_ID FlinkDeveloper on $ENV_ID"
confluent iam rbac role-binding create --principal "User:$SA_ID" --role FlinkDeveloper \
  --environment "$ENV_ID" >/dev/null 2>&1 \
  || echo "    (role binding already exists or could not be created; check it with 'confluent iam rbac role-binding list')" >&2

step "Granting $SA_ID DeveloperRead and DeveloperWrite on all topics of $CLUSTER_ID"
for role in DeveloperRead DeveloperWrite; do
  confluent iam rbac role-binding create --principal "User:$SA_ID" --role "$role" \
    --resource 'Topic:*' --environment "$ENV_ID" --cloud-cluster "$CLUSTER_ID" --kafka-cluster "$CLUSTER_ID" >/dev/null 2>&1 \
    || echo "    ($role: role binding already exists or could not be created; check it with 'confluent iam rbac role-binding list')" >&2
done

step "Granting $SA_ID MetricsViewer on the organization"
confluent iam rbac role-binding create --principal "User:$SA_ID" --role MetricsViewer >/dev/null 2>&1 \
  || echo "    (role binding already exists or could not be created; check it with 'confluent iam rbac role-binding list')" >&2

step "Creating a Schema Registry API key"
sr_key_json=$(confluent api-key create --resource "$sr_id" --service-account "$SA_ID" \
  --environment "$ENV_ID" --description "common lab Schema Registry key for $SA_ID" -o json)

step "Creating a Cloud API key for the Metrics API"
cloud_key_json=$(confluent api-key create --resource cloud --service-account "$SA_ID" \
  --description "common lab Cloud API key (Metrics API) for $SA_ID" -o json)

# Set NAME=value in .env: replace the line if NAME is there, else append it.
ENV_FILE="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)/.env"
set_env() {
  local tmp
  [[ -f "$ENV_FILE" ]] || : >"$ENV_FILE"
  tmp=$(mktemp "$ENV_FILE.XXXXXX")
  NAME=$1 VALUE=$2 awk 'BEGIN { n = ENVIRON["NAME"]; v = ENVIRON["VALUE"] }
    index($0, n "=") == 1 { print n "=" v; done = 1; next }
    { print }
    END { if (!done) print n "=" v }' "$ENV_FILE" >"$tmp"
  chmod 600 "$tmp"
  mv "$tmp" "$ENV_FILE"
}

bootstrap=$(jq -r '.endpoint | sub("^SASL_SSL://"; "")' <<<"$cluster_json")
cat <<EOF

# ---- Kafka cluster $CLUSTER_ID in $ENV_ID (service account $SA_ID) ----
KAFKA_CLUSTER_ID=$CLUSTER_ID
KAFKA_BOOTSTRAP=$bootstrap
KAFKA_REST_ENDPOINT=$(jq -r '.rest_endpoint' <<<"$cluster_json")
KAFKA_API_KEY=$(jq -r '.api_key' <<<"$kafka_key_json")
KAFKA_API_SECRET=$(jq -r '.api_secret' <<<"$kafka_key_json")

# ---- Schema Registry ----
SCHEMA_REGISTRY_CLUSTER_ID=$sr_id
SCHEMA_REGISTRY_ENDPOINT=$(jq -r '.endpoint_url' <<<"$sr_json")
SCHEMA_REGISTRY_PRIVATE_ENDPOINT=$(jq -r '.private_endpoint_url // empty' <<<"$sr_json")
SCHEMA_REGISTRY_API_KEY=$(jq -r '.api_key' <<<"$sr_key_json")
SCHEMA_REGISTRY_API_SECRET=$(jq -r '.api_secret' <<<"$sr_key_json")
EOF
step "Updating $ENV_FILE"
set_env BOOTSTRAP_SERVERS "$bootstrap"
set_env REST_ENDPOINT "$(jq -r '.rest_endpoint' <<<"$cluster_json")"
set_env KEY "$(jq -r '.api_key' <<<"$kafka_key_json")"
set_env SECRET "$(jq -r '.api_secret' <<<"$kafka_key_json")"
set_env SCHEMA_REGISTRY_ENDPOINT "$(jq -r '.endpoint_url' <<<"$sr_json")"
set_env SCHEMA_REGISTRY_KEY "$(jq -r '.api_key' <<<"$sr_key_json")"
set_env SCHEMA_REGISTRY_SECRET "$(jq -r '.api_secret' <<<"$sr_key_json")"
set_env CLOUD_API_KEY "$(jq -r '.api_key' <<<"$cloud_key_json")"
set_env CLOUD_API_SECRET "$(jq -r '.api_secret' <<<"$cloud_key_json")"
[[ -z "$POOL_ID" ]] || set_env FLINK_COMPUTE_POOL_ID "$POOL_ID"
echo "==> Done. Hand the keys to the students privately; the secrets cannot be shown again." >&2
