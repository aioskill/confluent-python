#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  invite_users_and_assign_roles.sh EMAIL_FILE ENVIRONMENT_ID KAFKA_CLUSTER_ID SCHEMA_REGISTRY_CLUSTER_ID

EMAIL_FILE contains one email address per line. Blank lines and lines whose
first non-space character is # are ignored.

Example:
  ./invite_users_and_assign_roles.sh users.txt env-abc123 lkc-abc123 lsrc-abc123

The Confluent CLI must be installed and already authenticated with an account
that can invite users and create role bindings.
EOF
}

if [[ $# -ne 4 ]]; then
  usage >&2
  exit 2
fi

email_file=$1
environment_id=$2
kafka_cluster_id=$3
schema_registry_cluster_id=$4

if ! command -v confluent >/dev/null 2>&1; then
  echo "Error: confluent CLI was not found in PATH." >&2
  exit 1
fi

if [[ ! -r "$email_file" || ! -f "$email_file" ]]; then
  echo "Error: email file is not a readable regular file: $email_file" >&2
  exit 1
fi

if [[ ! "$environment_id" =~ ^env-[[:alnum:]-]+$ ]]; then
  echo "Error: environment ID should look like env-xxxxxx." >&2
  exit 1
fi

if [[ ! "$kafka_cluster_id" =~ ^lkc-[[:alnum:]-]+$ ]]; then
  echo "Error: Kafka cluster ID should look like lkc-xxxxxx." >&2
  exit 1
fi

if [[ ! "$schema_registry_cluster_id" =~ ^lsrc-[[:alnum:]-]+$ ]]; then
  echo "Error: Schema Registry cluster ID should look like lsrc-xxxxxx." >&2
  exit 1
fi

declare -a emails=()
declare -A seen_emails=()
line_number=0

while IFS= read -r line || [[ -n "$line" ]]; do
  ((line_number += 1))
  email="${line#"${line%%[![:space:]]*}"}"
  email="${email%"${email##*[![:space:]]}"}"

  [[ -z "$email" || "$email" == \#* ]] && continue

  if [[ ! "$email" =~ ^[^[:space:]@]+@[^[:space:]@]+\.[^[:space:]@]+$ ]]; then
    echo "Error: invalid email on line $line_number: $email" >&2
    exit 1
  fi

  normalized_email="${email,,}"
  if [[ -n "${seen_emails[$normalized_email]:-}" ]]; then
    echo "Error: duplicate email on line $line_number: $email" >&2
    exit 1
  fi

  seen_emails[$normalized_email]=1
  emails+=("$email")
done < "$email_file"

if [[ ${#emails[@]} -eq 0 ]]; then
  echo "Error: no email addresses found in $email_file." >&2
  exit 1
fi

echo "Will invite ${#emails[@]} user(s) and assign:"
echo "  CloudClusterAdmin on $kafka_cluster_id"
echo "  ResourceOwner on all subjects in $schema_registry_cluster_id"
echo "  Environment: $environment_id"

for email in "${emails[@]}"; do
  echo "Inviting $email..."
  confluent iam user invitation create "$email"

  echo "Assigning CloudClusterAdmin to $email..."
  confluent iam rbac role-binding create \
    --principal "User:$email" \
    --role CloudClusterAdmin \
    --environment "$environment_id" \
    --cloud-cluster "$kafka_cluster_id"

  echo "Assigning ResourceOwner on all Schema Registry subjects to $email..."
  confluent iam rbac role-binding create \
    --principal "User:$email" \
    --role ResourceOwner \
    --environment "$environment_id" \
    --schema-registry-cluster "$schema_registry_cluster_id" \
    --resource 'Subject:*'
done

echo "Completed invitations and role assignments for ${#emails[@]} user(s)."
