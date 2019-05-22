#!/bin/sh

# embarcacao
solrctl --zk localhost:2181/solr collection --delete embarcacao
solrctl --zk localhost:2181/solr instancedir --delete embarcacao 

solrctl --zk localhost:2181/solr instancedir --create embarcacao schema/solr/embarcacao
solrctl --zk localhost:2181/solr collection --create embarcacao -s 3 -m 3 

# veiculo
solrctl --zk localhost:2181/solr collection --delete veiculo
solrctl --zk localhost:2181/solr instancedir --delete veiculo

solrctl --zk localhost:2181/solr instancedir --create veiculo schema/solr/veiculo
solrctl --zk localhost:2181/solr collection --create veiculo -s 3 -m 3 

# pessoa juridica
solrctl  collection --delete pessoa_juridica
solrctl  instancedir --delete pessoa_juridica 

solrctl  instancedir --create pessoa_juridica schema/solr/pessoa_juridica
solrctl  collection --create pessoa_juridica -s 3 -m 3 

# pessoa fisica
solrctl  collection --delete pessoa_fisica
solrctl  instancedir --delete pessoa_fisica 

solrctl  instancedir --create pessoa_fisica schema/solr/pessoa_fisica
solrctl  collection --create pessoa_fisica -s 3 -m 3 

# documento_personagem
solrctl  collection --delete documento_personagem
solrctl  instancedir --delete documento_personagem 

solrctl  instancedir --create documento_personagem schema/solr/documento_personagem
solrctl  collection --create documento_personagem -s 3 -m 3 



# export PYTHONIOENCODING=utf8

# spark2-submit --py-files src/opg_utils.py,src/timer.py,packages/*.whl \
#         --conf spark.yarn.queue="root.mpmapas" \
#         --conf spark.yarn.executor.memoryOverhead=6g \
#         src/generate_solr.py 2>> error.log
