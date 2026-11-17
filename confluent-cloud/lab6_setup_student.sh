#!/usr/bin/env bash
# Instructor script for Labs 6A and 6B: give one student a service account whose Kafka
# permissions are limited to resources whose names start with "<STUDENT_ID>-".
#
# Usage:
#   ./lab6_setup_student.sh create <STUDENT_ID> <ENVIRONMENT_ID> <KAFKA_CLUSTER_ID>
#   ./lab6_setup_student.sh delete <STUDENT_ID> <ENVIRONMENT_ID> <KAFKA_CLUSTER_ID>
#
# "create" writes the student's API key and secret to lab6-students/<ID>.txt
# (mode 0600, git-ignored). Hand that file to the student privately.
# Requires `confluent login` as a user who may create service accounts, API
# keys, and ACLs on the cluster.

set -euo pipefail

if [[ $# -ne 4 ]] || [[ "$1" != "create" && "$1" != "delete" ]]; then
  sed -n '2,12p' "$0" >&2
  exit 2
fi

action=$1
student=$2
environment=$3
cluster=$4

if [[ ! "$student" =~ ^[a-z][a-z0-9]{1,15}$ ]]; then
  echo "Error: STUDENT_ID must be lowercase letters/digits, 2-16 chars, starting with a letter." >&2
  exit 1
fi

sa_name="lab6-${student}"
out_dir="$(dirname "$0")/lab6-students"
out_file="${out_dir}/${student}.txt"
common=(--environment "$environment" --cluster "$cluster")

sa_id() {
  confluent iam service-account list -o json |
    python3 -c '
import json, sys
name = sys.argv[1]
for sa in json.load(sys.stdin):
    if sa.get("name") == name:
        print(sa["id"])
        break
' "$sa_name"
}

# ACLs: topics and consumer groups starting with "<student>-", plus the
# cluster-level idempotent-write permission that Connect producers need.
acl() {
  local verb=$1 sa=$2 force=()
  [[ "$verb" == "delete" ]] && force=(--force)
  confluent kafka acl "$verb" --allow --service-account "$sa" "${common[@]}" "${force[@]}" \
    --operations read,write,create,describe,describe-configs,alter-configs,delete \
    --topic "${student}-" --prefix
  confluent kafka acl "$verb" --allow --service-account "$sa" "${common[@]}" "${force[@]}" \
    --operations read,describe,delete \
    --consumer-group "${student}-" --prefix
  confluent kafka acl "$verb" --allow --service-account "$sa" "${common[@]}" "${force[@]}" \
    --operations idempotent-write --cluster-scope
  # A self-managed Confluent Connect worker stores its license in this
  # fixed topic name, shared by all students.
  confluent kafka acl "$verb" --allow --service-account "$sa" "${common[@]}" "${force[@]}" \
    --operations read,write,create,describe,describe-configs \
    --topic "_confluent-command"
  # A fully managed SINK connector reads with the consumer group
  # "connect-<connector id>" (ids look like lcc-abc123). The id is unknown until
  # the connector exists, so grant the shared prefix "connect-lcc-" (it covers
  # every connector's group in this environment, not only the student's).
  confluent kafka acl "$verb" --allow --service-account "$sa" "${common[@]}" "${force[@]}" \
    --operations read,describe,delete \
    --consumer-group "connect-lcc-" --prefix
  # A self-managed *standalone* sink worker (Lab 6C) reads with the group
  # "connect-<connector name>", and the connector name starts with "<student>-".
  confluent kafka acl "$verb" --allow --service-account "$sa" "${common[@]}" "${force[@]}" \
    --operations read,describe,delete \
    --consumer-group "connect-${student}-" --prefix
}

if [[ "$action" == "create" ]]; then
  id=$(sa_id)
  if [[ -z "$id" ]]; then
    confluent iam service-account create "$sa_name" \
      --description "Lab 6 student ${student}" -o json >/dev/null
    id=$(sa_id)
  fi
  acl create "$id"

  install -d -m 0700 "$out_dir"
  install -m 0600 /dev/null "$out_file"
  confluent api-key create --resource "$cluster" --service-account "$id" \
    --environment "$environment" -o json |
    python3 -c '
import json, sys
k = json.load(sys.stdin)
print("student=" + sys.argv[1])
print("service_account=" + sys.argv[2])
print("kafka_api_key=" + k["api_key"])
print("kafka_api_secret=" + k["api_secret"])
' "$student" "$id" >"$out_file"
  echo "Created ${sa_name} (${id}); credentials in ${out_file}"
else
  id=$(sa_id)
  if [[ -z "$id" ]]; then
    echo "No service account ${sa_name}; nothing to delete."
    exit 0
  fi
  confluent api-key list --resource "$cluster" --service-account "$id" -o json |
    python3 -c '
import json, sys
for k in json.load(sys.stdin):
    print(k["key"])
' | while read -r key; do
      confluent api-key delete "$key" --force
    done
  acl delete "$id" || true
  confluent iam service-account delete "$id" --force
  rm -f "$out_file"
  echo "Deleted ${sa_name}"
fi
