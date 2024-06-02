#!/bin/bash
set -ex  # For debugging the script

SCRIPT_DIR=$(dirname $(realpath $0))

NAME="cassandra1"
EXPOSEPORT=9042

docker exec -i $NAME cqlsh -u cqlsh -p $EXPOSEPORT -t < ${SCRIPT_DIR}/load-shop_db-data.cql

echo "LOAD DATA SUCCESS"