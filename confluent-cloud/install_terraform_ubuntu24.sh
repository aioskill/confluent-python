#!/usr/bin/env bash

set -euo pipefail

readonly signing_key_url="https://apt.releases.hashicorp.com/gpg"
readonly expected_fingerprint="D55C0D1AC78A8D8126CB631CFC9CA96ACA026560"
readonly keyring_path="/usr/share/keyrings/hashicorp-archive-keyring.gpg"
readonly repository_path="/etc/apt/sources.list.d/hashicorp.list"

if [[ ! -r /etc/os-release ]]; then
  echo "Error: cannot identify this operating system (/etc/os-release missing)." >&2
  exit 1
fi

# shellcheck disable=SC1091
source /etc/os-release
if [[ "${ID:-}" != "ubuntu" || "${VERSION_ID:-}" != 24.* ]]; then
  echo "Error: this installer supports Ubuntu 24.x only; found ${PRETTY_NAME:-unknown}." >&2
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
tmp_keyring=$(mktemp)
trap 'rm -f "$tmp_key" "$tmp_keyring"' EXIT

"${apt_cmd[@]}" update
"${apt_cmd[@]}" install -y ca-certificates curl gnupg

curl -fsSL "$signing_key_url" --output "$tmp_key"
actual_fingerprint=$(gpg --show-keys --with-colons "$tmp_key" \
  | awk -F: '$1 == "fpr" { print $10; exit }')
if [[ "$actual_fingerprint" != "$expected_fingerprint" ]]; then
  echo "Error: HashiCorp signing key fingerprint did not match the expected value." >&2
  exit 1
fi

gpg --dearmor --yes --output "$tmp_keyring" "$tmp_key"
"${root_cmd[@]}" install -d -m 0755 /usr/share/keyrings
"${root_cmd[@]}" install -o root -g root -m 0644 "$tmp_keyring" "$keyring_path"

ubuntu_codename="${UBUNTU_CODENAME:-${VERSION_CODENAME:-}}"
if [[ -z "$ubuntu_codename" ]]; then
  echo "Error: unable to determine the Ubuntu release codename." >&2
  exit 1
fi
architecture=$(dpkg --print-architecture)
repository_line="deb [arch=${architecture} signed-by=${keyring_path}] https://apt.releases.hashicorp.com ${ubuntu_codename} main"

if [[ ! -f "$repository_path" ]] || [[ "$(<"$repository_path")" != "$repository_line" ]]; then
  printf '%s\n' "$repository_line" | "${root_cmd[@]}" tee "$repository_path" >/dev/null
fi

"${apt_cmd[@]}" update
"${apt_cmd[@]}" install -y terraform

echo "Terraform installed:"
terraform version
