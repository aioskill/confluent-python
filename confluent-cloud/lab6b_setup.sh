#!/usr/bin/env bash
# Instructor script for Lab 6B: the PostgreSQL database and the JDBC connector
# plugin on the lab hosts. Every student has the same database login.
#
# Usage:
#   ./lab6b_setup.sh init      # PostgreSQL on sa01 (database and login lab6b); psql + JDBC plugin on sa01-sa03
#   ./lab6b_setup.sh delete    # drop the database and login
#
# Requires passwordless ssh as ubuntu to the lab hosts, with sudo.

set -euo pipefail

DB_HOST=sa01
PLUGIN_HOSTS="sa01 sa02 sa03"
LAB_NET=192.168.120.0/24      # the lab hosts' network; only it may reach PostgreSQL
DB_NAME=lab6b
DB_USER=lab6b
DB_PASSWORD=lab6b-pass
COMPONENTS=/app/confluent/share/confluent-hub-components

usage() { sed -n '2,9p' "$0" >&2; exit 2; }
[[ $# -eq 1 ]] || usage
action=$1

pg() { ssh "ubuntu@${DB_HOST}" "sudo -u postgres psql -v ON_ERROR_STOP=1 -q -X $*"; }

if [[ "$action" == "init" ]]; then
  ssh "ubuntu@${DB_HOST}" 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql >/dev/null 2>&1'
  ssh "ubuntu@${DB_HOST}" "set -e
    conf=\$(ls -d /etc/postgresql/*/main)
    echo \"listen_addresses = '*'\" | sudo tee \$conf/conf.d/lab6b.conf >/dev/null
    line='host ${DB_NAME} all ${LAB_NET} scram-sha-256'
    sudo grep -qxF \"\$line\" \$conf/pg_hba.conf || echo \"\$line\" | sudo tee -a \$conf/pg_hba.conf >/dev/null
    sudo systemctl restart postgresql"
  # The login owns the database, so it can create tables in the public schema.
  pg -c "\"DO \\\$\\\$ BEGIN IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='${DB_USER}') THEN CREATE ROLE ${DB_USER} LOGIN PASSWORD '${DB_PASSWORD}'; END IF; END \\\$\\\$\""
  exists=$(pg -tAc "\"SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'\"")
  [[ "$exists" == "1" ]] || pg -c "\"CREATE DATABASE ${DB_NAME} OWNER ${DB_USER}\""
  pg -c "\"ALTER DATABASE ${DB_NAME} OWNER TO ${DB_USER}\""
  for h in $PLUGIN_HOSTS; do
    ssh "ubuntu@${h}" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql-client >/dev/null 2>&1
      sudo mkdir -p ${COMPONENTS}
      ls ${COMPONENTS} | grep -q kafka-connect-jdbc || sudo /app/confluent/bin/confluent-hub install --no-prompt \
        --component-dir ${COMPONENTS} --worker-configs /dev/null confluentinc/kafka-connect-jdbc:latest >/dev/null"
    echo "psql client and JDBC plugin ready on ${h}"
  done
  echo "PostgreSQL ready on ${DB_HOST}:5432, database ${DB_NAME}"
  exit 0
fi

if [[ "$action" == "delete" ]]; then
  pg -c "\"DROP DATABASE IF EXISTS ${DB_NAME}\"" -c "\"DROP ROLE IF EXISTS ${DB_USER}\""
  echo "Deleted database ${DB_NAME} and login ${DB_USER}"
  exit 0
fi
usage
