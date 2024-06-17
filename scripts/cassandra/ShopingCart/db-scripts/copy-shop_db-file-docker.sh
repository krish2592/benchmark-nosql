#!/bin/bash

set -ex 

cd "$(dirname ${BASH_SOURCE[0]})/../../../../data/ShopingCart/project_files/data_files"

echo "$(pwd)"

docker exec -it cassandra1 mkdir -p /shoping-data

docker exec -it cassandra1 rm -r /shoping-data/

docker cp . cassandra1:/shoping-data/

docker cut -d ',' -f 1,2,3 /shoping-data/item.csv > /shoping-data/items_filtered.csv # Have to test it
docker cut -d ',' -f 1,4,5 /shoping-data/item.csv > /shoping-data/items_unused_filtered.csv  # Have to test it

echo "Copied Suceess"
