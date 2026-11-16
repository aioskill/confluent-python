#!/usr/bin/env bash

set -euo pipefail

readonly EXCLUDED_ADMIN_EMAIL='training@avalanchio.com'

usage() {
  cat <<'USAGE'
Usage:
  assign_roles.sh [--yes]

Fetches all current Confluent Cloud users and ensures each user except
training@avalanchio.com has the OrganizationAdmin role. This role grants full
access across the organization, including creating, managing, and deleting
environments, Kafka clusters, and Schema Registry resources.

Existing OrganizationAdmin bindings are left unchanged, so rerunning the
script is safe. Without --yes, the script displays the target users and asks
for confirmation before creating any role bindings.

The Confluent CLI must be installed and logged in. jq is required to parse CLI
JSON output. Users must already exist in the organization; pending invitations
are not returned by `confluent iam user list`.
USAGE
}

confirm='true'
if [[ ${1:-} == '--yes' ]]; then
  confirm='false'
  shift
fi

if [[ $# -ne 0 ]]; then
  usage >&2
  exit 2
fi

if ! command -v confluent >/dev/null 2>&1; then
  echo 'Error: confluent CLI was not found in PATH.' >&2
  exit 1
fi
if ! command -v jq >/dev/null 2>&1; then
  echo 'Error: jq is required to parse Confluent CLI JSON output.' >&2
  exit 1
fi

echo 'Fetching the current Confluent Cloud user list...'
users_json=$(confluent iam user list -o json)
if ! jq -e 'type == "array"' >/dev/null <<< "$users_json"; then
  echo 'Error: Confluent CLI returned an unexpected user-list format.' >&2
  exit 1
fi

declare -a user_ids=()
declare -a user_emails=()
while IFS=$'\t' read -r user_id user_email; do
  [[ -z $user_id || -z $user_email ]] && continue
  [[ ${user_email,,} == "$EXCLUDED_ADMIN_EMAIL" ]] && continue
  user_ids+=("$user_id")
  user_emails+=("$user_email")
done < <(jq -r '.[] | select((.id | type) == "string" and (.email | type) == "string") | [.id, .email] | @tsv' <<< "$users_json")

if [[ ${#user_ids[@]} -eq 0 ]]; then
  echo "Error: no users found after excluding $EXCLUDED_ADMIN_EMAIL." >&2
  exit 1
fi

# Fetch existing bindings before making changes so a list/permission error
# cannot leave only part of the user set updated.
declare -A bindings_by_user=()
echo "Found ${#user_ids[@]} eligible users. Checking existing OrganizationAdmin bindings..."
total_users=${#user_ids[@]}
checked_users=0
for index in "${!user_ids[@]}"; do
  user_id=${user_ids[$index]}
  ((checked_users += 1))
  printf '[%d/%d] Checking %s...\n' "$checked_users" "$total_users" "${user_emails[$index]}"
  bindings_by_user[$user_id]=$(confluent iam rbac role-binding list \
    --principal "User:$user_id" --inclusive -o json)
  if ! jq -e 'type == "array"' >/dev/null <<< "${bindings_by_user[$user_id]}"; then
    echo "Error: unexpected role-binding list format for User:$user_id." >&2
    exit 1
  fi
done

echo "Will ensure OrganizationAdmin for ${#user_ids[@]} user(s), excluding $EXCLUDED_ADMIN_EMAIL:"
for index in "${!user_ids[@]}"; do
  printf '  %s (%s)\n' "${user_emails[$index]}" "${user_ids[$index]}"
done
echo 'OrganizationAdmin grants full access across this Confluent Cloud organization.'

if [[ $confirm == 'true' ]]; then
  read -r -p 'Type YES to continue: ' confirmation
  if [[ $confirmation != 'YES' ]]; then
    echo 'Cancelled; no role bindings were created.'
    exit 0
  fi
fi

for index in "${!user_ids[@]}"; do
  user_id=${user_ids[$index]}
  email=${user_emails[$index]}
  bindings_json=${bindings_by_user[$user_id]}
  echo "Checking existing role bindings for $email..."
  if jq -e '
    any(.[]; .role == "OrganizationAdmin" and
      (.environment // "") == "" and
      (.cloud_cluster // "") == "" and
      (.logical_cluster // "") == "" and
      (.resource_type // "") == "")
  ' >/dev/null <<< "$bindings_json"; then
    echo "Already present: OrganizationAdmin for $email"
    continue
  fi

  echo "Assigning OrganizationAdmin to $email..."
  confluent iam rbac role-binding create \
    --principal "User:$user_id" \
    --role OrganizationAdmin
done

echo "Completed OrganizationAdmin checks for ${#user_ids[@]} user(s)."
