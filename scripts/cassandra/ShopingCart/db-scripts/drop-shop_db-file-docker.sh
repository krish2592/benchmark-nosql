#!/bin/bash
set -ex  # For debugging the script

SCRIPT_DIR=$(dirname $(realpath $0))

NAME="cassandra1"
EXPOSEPORT=9042

docker exec -i $NAME cqlsh -u cqlsh -p $EXPOSEPORT -t < ${SCRIPT_DIR}/drop-shop_db-file-docker.cql