#!/usr/bin/env bash
# Instructor script for Lab 6B. Run it ON sa01 (as ubuntu, with sudo); it does not
# use ssh. Everything for the lab is on sa01: PostgreSQL, the psql client and the
# JDBC connector plugin. Every student has the same database login.
#
# Usage:
#   bash lab6b_setup.sh init     # PostgreSQL, database lab6b and login lab6b, psql client, JDBC plugin
#   bash lab6b_setup.sh delete   # drop the database and login
#
# Copy it to sa01 first, for example: scp lab6b_setup.sh ubuntu@sa01:

set -euo pipefail

DB_HOST=sa01
LAB_NET=192.168.120.0/24      # the lab hosts' network; only it may reach PostgreSQL
DB_NAME=lab6b
DB_USER=lab6b
DB_PASSWORD=lab6b-pass
COMPONENTS=/app/confluent/share/confluent-hub-components

usage() { sed -n '2,10p' "$0" >&2; exit 2; }
[[ $# -eq 1 ]] || usage
action=$1
here=$(hostname -s)
[[ "$here" == "$DB_HOST" ]] || { echo "Error: run this script on ${DB_HOST}, not on ${here}." >&2; exit 1; }

pg() { sudo -u postgres psql -v ON_ERROR_STOP=1 -q -X "$@"; }

case "$action" in
  init)
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get install -y postgresql >/dev/null
    conf=$(ls -d /etc/postgresql/*/main)
    # Bind to this host's address on the lab network (and localhost), not to every interface.
    bind_ip=$(ip -4 -o addr show | awk '{print $4}' | cut -d/ -f1 | grep "^${LAB_NET%.*.*}\." | head -1)
    [[ -n "$bind_ip" ]] || { echo "Error: no address in ${LAB_NET} on ${here}." >&2; exit 1; }
    echo "listen_addresses = 'localhost,${bind_ip}'" | sudo tee "$conf/conf.d/lab6b.conf" >/dev/null
    line="host ${DB_NAME} all ${LAB_NET} scram-sha-256"
    sudo grep -qxF "$line" "$conf/pg_hba.conf" || echo "$line" | sudo tee -a "$conf/pg_hba.conf" >/dev/null
    sudo systemctl restart postgresql
    # The login owns the database, so it can create tables in the public schema.
    pg <<SQL
DO \$\$ BEGIN
IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${DB_USER}') THEN
  CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}';
END IF;
END \$\$;
SQL
    [[ "$(pg -tAc "SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'")" == "1" ]] \
      || pg -c "CREATE DATABASE ${DB_NAME} OWNER ${DB_USER}"
    pg -c "ALTER DATABASE ${DB_NAME} OWNER TO ${DB_USER}"
    echo "PostgreSQL ready on ${bind_ip}:5432, database ${DB_NAME}"
    sudo apt-get install -y postgresql-client >/dev/null
    sudo mkdir -p "$COMPONENTS"
    ls "$COMPONENTS" | grep -q kafka-connect-jdbc || sudo /app/confluent/bin/confluent-hub install --no-prompt \
      --component-dir "$COMPONENTS" --worker-configs /dev/null confluentinc/kafka-connect-jdbc:latest >/dev/null
    echo "psql client and JDBC plugin ready on ${here}"
    ;;
  delete)
    pg -c "DROP DATABASE IF EXISTS ${DB_NAME}" -c "DROP ROLE IF EXISTS ${DB_USER}"
    echo "Deleted database ${DB_NAME} and login ${DB_USER}"
    ;;
  *) usage ;;
esac
