#!/bin/bash

set -ex 

cd "$(dirname ${BASH_SOURCE[0]})/../../../../workloads/ShopingCart/project_files/data_files"

echo "$(pwd)"

docker exec -it cassandra1 mkdir -p /shoping-data

docker exec -it cassandra1 rm -r /shoping-data/

docker cp . cassandra1:/shoping-data/

echo "Copied Suceess"
