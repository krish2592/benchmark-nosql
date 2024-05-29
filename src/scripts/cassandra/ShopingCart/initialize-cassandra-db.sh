#!/bin/bash
set -ex  # For debugging the script

SCRIPT_DIR=$(dirname $(realpath $0))

# Check if the Docker image exists
if [[ -z $(docker images --filter=reference='cassandra:4.1.5' -q) ]]; then
  # Pull the cassendra image
  docker pull cassandra:4.1.5
  docker network create cassandra_nw
else
  echo "Image cassandra:4.1.5 already exists."
fi

# Check if the Cassandra Docker network exists
if [[ -z $(docker network ls --filter name='cassandra_nw' -q) ]]; then
  # Create network
  docker network create cassandra_nw
else
  echo "Network already created"
fi


# docker images | grep cassandra | grep 4.1.5
NAME="cassandra1"
EXPOSEPORT=9042

sleep 10

docker exec -i $NAME cqlsh -u cqlsh -p $EXPOSEPORT -t < ${SCRIPT_DIR}/create-keyspaces-cassandra-db.cql

echo "Completed!"
