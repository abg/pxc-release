#!/usr/bin/env bash

set -euo pipefail

mkdir -p /var/log/mysql

initialize_mysql_datadir() {
    mysqld --initialize-insecure \
           --disable-log-error \
           --init-file=/usr/local/bin/init.sql \
           --datadir=/var/lib/mysql
}

initialize_mysql_datadir

exec /usr/local/bin/mysql-controller --config=/etc/mysql-controller/config.yaml
