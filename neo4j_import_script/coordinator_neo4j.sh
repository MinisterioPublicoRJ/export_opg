#!/bin/bash

sudo chmod a+rw /neo4j/data/databases/
sudo rm -rf /neo4j/data/databases/graph.db.back
sudo rm -rf /neo4j/data/databases/temp_graph.db/

COMMAND_IMPORT=$(docker exec $(docker ps -f 'name=neo4j' -q) /import/export_opg/neo4j_import_script/generate_neo4j_import_string.sh /import/neo4j)

docker exec  $(docker ps -f 'name=neo4j' -q) $COMMAND_IMPORT

docker stop $(docker ps -f 'name=neo4j' -q)
docker rm neo4j

sudo chmod a+rw -R /neo4j/data/databases/temp_graph.db/
sudo mv /neo4j/data/databases/graph.db/ /neo4j/data/databases/graph.db.back
sudo chmod a+rw -R /neo4j/data/databases/graph.db.back
sudo mv /neo4j/data/databases/temp_graph.db/ /neo4j/data/databases/graph.db
sudo chmod a+rw -R /neo4j/data/databases/graph.db

docker run --name neo4j \
    -p 80:7474 \
    -p 443:7473 \
    -p 7687:7687 \
    -d \
    -v /neo4j/data:/data \
    -v /neo4j/plugins:/plugins \
    -v /neo4j/import:/import \
    -v /neo4j/export:/export \
    --env=NEO4J_apoc_export_file_enabled=true \
    --env=NEO4J_dbms_memory_heap_maxSize=31G \
    --env=NEO4J_dbms_memory_pagecache_size=143G \
    --env=NEO4J_dbms_jvm_additional=-XX:+UseG1GC \
    --env=NEO4J_apoc_export_file_enabled=true \
    --env=NEO4J_apoc_export_file_enabled=true \
    --env=NEO4J_PASSWD=$NEO4J_PASSWD \
    neo4j:3.5.2 

./generate_neo4j_indexes.sh
