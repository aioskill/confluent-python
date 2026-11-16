#!/usr/bin/env bash

set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  invite_users.sh EMAIL_FILE

EMAIL_FILE contains one email address per line. Blank lines and lines whose
first non-space character is # are ignored.

Example:
  ./invite_users.sh users.txt

The Confluent CLI must be installed and already authenticated with an account
that can invite users. Invitees must accept their invitations before running
assign_roles.sh.
EOF
}

if [[ $# -ne 1 ]]; then
  usage >&2
  exit 2
fi

email_file=$1

if ! command -v confluent >/dev/null 2>&1; then
  echo "Error: confluent CLI was not found in PATH." >&2
  exit 1
fi

if [[ ! -r "$email_file" || ! -f "$email_file" ]]; then
  echo "Error: email file is not a readable regular file: $email_file" >&2
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

echo "Will invite ${#emails[@]} user(s)."

failed=0
for email in "${emails[@]}"; do
  echo "Inviting $email..."
  if ! confluent iam user invitation create "$email"; then
    echo "Error: invitation failed for $email; continuing with the remaining users." >&2
    ((failed += 1))
  fi
done

if (( failed > 0 )); then
  echo "Completed with $failed invitation failure(s) out of ${#emails[@]} user(s)." >&2
  exit 1
fi

echo "Completed invitations for ${#emails[@]} user(s)."
