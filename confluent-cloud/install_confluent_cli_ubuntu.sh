#!/usr/bin/env bash

set -euo pipefail

readonly key_url="https://packages.confluent.io/confluent-cli/deb/archive.key"
readonly keyring_path="/etc/apt/keyrings/confluent-cli.gpg"
readonly repository_path="/etc/apt/sources.list.d/confluent-cli.list"
readonly repository_line="deb [signed-by=${keyring_path}] https://packages.confluent.io/confluent-cli/deb stable main"

if [[ ! -r /etc/os-release ]]; then
  echo "Error: cannot identify this operating system (/etc/os-release missing)." >&2
  exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
ubuntu_major="${VERSION_ID%%.*}"
if [[ "${ID:-}" != "ubuntu" || ! "${ubuntu_major}" =~ ^[0-9]+$ || "${ubuntu_major}" -lt 22 ]]; then
  echo "Error: this installer supports Ubuntu 22.04 or newer; found ${PRETTY_NAME:-unknown}." >&2
  exit 1
fi

if [[ "${EUID}" -eq 0 ]]; then
  apt_cmd=(apt-get)
  root_cmd=()
else
  if ! command -v sudo >/dev/null 2>&1; then
    echo "Error: run this script as root or install sudo and rerun it." >&2
    exit 1
  fi
  apt_cmd=(sudo apt-get)
  root_cmd=(sudo)
fi

tmp_key=$(mktemp)
trap 'rm -f "$tmp_key"' EXIT

"${apt_cmd[@]}" update
"${apt_cmd[@]}" install -y ca-certificates curl gnupg

"${root_cmd[@]}" install -d -m 0755 /etc/apt/keyrings
curl -fsSL "$key_url" | gpg --dearmor --yes --output "$tmp_key"
"${root_cmd[@]}" install -o root -g root -m 0644 "$tmp_key" "$keyring_path"

if [[ ! -f "$repository_path" ]] || [[ "$(<"$repository_path")" != "$repository_line" ]]; then
  printf '%s\n' "$repository_line" | "${root_cmd[@]}" tee "$repository_path" >/dev/null
fi

"${apt_cmd[@]}" update
"${apt_cmd[@]}" install -y confluent-cli

echo "Confluent CLI installed:"
confluent version
