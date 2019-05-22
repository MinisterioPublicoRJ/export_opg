#!/bin/bash
echo "Aguardando subida do neo4j por um minutinho"
sleep 60

indexes () {
    docker exec -i $(docker ps -f 'name=neo4j' -q) cypher-shell -u neo4j -p $NEO4J_PASSWD 'call db.indexes()' | grep POPULATING
}

docker exec -i  $(docker ps -f 'name=neo4j' -q) cypher-shell -u neo4j -p $NEO4J_PASSWD  << FIM
create index on :Pessoa(uuid);
create index on :Documento(uuid);
create index on :Embarcacao(uuid);
create index on :Empresa(uuid);
create index on :Multa(uuid);
create index on :Orgao(uuid);
create index on :Personagem(uuid);
create index on :Veiculo(uuid);

CREATE INDEX ON :Pessoa (sensivel);
CREATE INDEX ON :Veiculo (sensivel);
CREATE INDEX ON :Embarcacao (sensivel);
CREATE INDEX ON :Empresa (sensivel);
FIM

echo "Aguardando geração dos índices no NEO4J"
while [ ! -z "$(indexes)" ];  do sleep 1; echo .; done

echo "Atualizando relações para 'sensível'"
docker exec -i  $(docker ps -f 'name=neo4j' -q) cypher-shell -u neo4j -p $NEO4J_PASSWD  << FIM
MATCH r = (n:Pessoa {sensivel:1})-[*..1]-(n2)
set n2.sensivel = 1;


MATCH  (n:Pessoa {sensivel:1})-[r *..1]-(n2)
UNWIND r AS x
set x.sensivel = true;

FIM


# docker exec -i  $(docker ps -f 'name=neo4j' -q) cypher-shell -u neo4j -p $NEO4J_PASSWD  << FIM
# call apoc.export.csv.query("match (n:Pessoa {sensivel:true}) return n.uuid","/export/solr_pessoa.csv",{stream:true,batchSize:100000})
# call apoc.export.csv.query("match (n:Empresa {sensivel:true}) return n.uuid","/export/solr_empresa.csv",{stream:true,batchSize:100000})
# call apoc.export.csv.query("match (n:Embarcacao {sensivel:true}) return n.uuid","/export/solr_embarcacao.csv",{stream:true,batchSize:100000})
# call apoc.export.csv.query("match (n:Veiculo {sensivel:true}) return n.uuid","/export/solr_veiculo.csv",{stream:true,batchSize:100000})
# FIM