./generate_uuids.sh
./generate_pessoa_fisica.sh
./generate_opvs.sh
./generate_connections.sh
./generate_connections_detran.sh
./generate_connections_mgp.sh
./generate_connections_mother.sh
./generate_connections_father.sh
./generate_connections_work.sh
./generate_connections_embarcacao.sh
./generate_neo4j.sh

echo "Copying from HDFS to Neo4J datastore"
./copy_csv_destination.sh

echo "Sending Neo4J import command"
ssh neo4j@neo4j.pgj.rj.gov.br 'bash -x /neo4j/import/export_opg/neo4j_import_script/coordinator_neo4j.sh > import.log 2> import_error.log &'