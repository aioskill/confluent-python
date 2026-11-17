#!/usr/bin/env bash
# Instructor script for Lab 6C: the PostgreSQL database and the JDBC connector
# plugin on the lab hosts, and one database login per student.
#
# Usage:
#   ./lab6c_setup.sh init                # once: PostgreSQL on sa01; psql + JDBC plugin on sa01-sa03
#   ./lab6c_setup.sh create <STUDENT_ID> # per student: login + schema, password saved locally
#   ./lab6c_setup.sh delete <STUDENT_ID>
#
# "create" writes the student's database settings to lab6c-students/<ID>.txt
# (mode 0600, git-ignored). Hand that file to the student privately.
# Requires passwordless ssh as ubuntu to the lab hosts, with sudo.

set -euo pipefail

DB_HOST=sa01
PLUGIN_HOSTS="sa01 sa02 sa03"
LAB_NET=192.168.120.0/24      # the lab hosts' network; only it may reach PostgreSQL
DB_NAME=lab6c
COMPONENTS=/app/confluent/share/confluent-hub-components

usage() { sed -n '2,12p' "$0" >&2; exit 2; }
[[ $# -ge 1 ]] || usage
action=$1

pg() { ssh "ubuntu@${DB_HOST}" "sudo -u postgres psql -v ON_ERROR_STOP=1 -q -X $*"; }

if [[ "$action" == "init" ]]; then
  ssh "ubuntu@${DB_HOST}" 'sudo DEBIAN_FRONTEND=noninteractive apt-get install -y postgresql >/dev/null 2>&1'
  ssh "ubuntu@${DB_HOST}" "set -e
    conf=\$(ls -d /etc/postgresql/*/main)
    echo \"listen_addresses = '*'\" | sudo tee \$conf/conf.d/lab6c.conf >/dev/null
    line='host ${DB_NAME} all ${LAB_NET} scram-sha-256'
    sudo grep -qxF \"\$line\" \$conf/pg_hba.conf || echo \"\$line\" | sudo tee -a \$conf/pg_hba.conf >/dev/null
    sudo systemctl restart postgresql"
  exists=$(pg -tAc "\"SELECT 1 FROM pg_database WHERE datname='${DB_NAME}'\"")
  [[ "$exists" == "1" ]] || pg -c "\"CREATE DATABASE ${DB_NAME}\""
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

[[ $# -eq 2 ]] || usage
student=$2
if [[ ! "$student" =~ ^[a-z][a-z0-9]{1,15}$ ]]; then
  echo "Error: STUDENT_ID must be lowercase letters/digits, 2-16 chars, starting with a letter." >&2
  exit 1
fi
out_dir="$(dirname "$0")/lab6c-students"
out_file="${out_dir}/${student}.txt"

case "$action" in
  create)
    install -d -m 0700 "$out_dir"
    if [[ -f "$out_file" ]]; then
      password=$(awk -F= '$1=="db_password"{print $2}' "$out_file")
    else
      password=$(openssl rand -hex 8)
    fi
    # The student owns a schema with their own name. PostgreSQL's default
    # search_path starts with the login's own name, so unqualified table names
    # land in that schema.
    ssh "ubuntu@${DB_HOST}" "sudo -u postgres psql -v ON_ERROR_STOP=1 -q -X -d ${DB_NAME}" <<SQL
DO \$\$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${student}') THEN
    CREATE ROLE "${student}" LOGIN PASSWORD '${password}';
  ELSE
    ALTER ROLE "${student}" PASSWORD '${password}';
  END IF;
END \$\$;
CREATE SCHEMA IF NOT EXISTS "${student}" AUTHORIZATION "${student}";
GRANT CONNECT ON DATABASE ${DB_NAME} TO "${student}";
SQL
    install -m 0600 /dev/null "$out_file"
    {
      echo "student=${student}"
      echo "db_host=${DB_HOST}"
      echo "db_port=5432"
      echo "db_name=${DB_NAME}"
      echo "db_user=${student}"
      echo "db_password=${password}"
    } >"$out_file"
    echo "Created database login ${student}; settings in ${out_file}"
    ;;
  delete)
    ssh "ubuntu@${DB_HOST}" "sudo -u postgres psql -v ON_ERROR_STOP=1 -q -X -d ${DB_NAME}" <<SQL
DO \$\$ BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = '${student}') THEN
    DROP OWNED BY "${student}";   -- the schema, its tables, and the connect grant
    DROP ROLE "${student}";
  END IF;
END \$\$;
SQL
    rm -f "$out_file"
    echo "Deleted database login ${student}"
    ;;
  *) usage ;;
esac
