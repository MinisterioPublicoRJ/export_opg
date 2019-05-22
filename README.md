# Ferramenta de exportação de dados e geração de OPV/OPE Oracle Spatial Graphs
<hr>

        /opt/cloudera/parcels/Anaconda-5.0.1/bin/pip download -r requirements.txt -d packages/
        tar xvzf packages/hdfs*.tar.gz
        cd hdfs*
        python setup.py bdist_egg
        mv dist/*.egg ../packages
        cd ..
        rm -r hdfs*

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
        ./generate_outputs.sh #pgx
        ./generate_neo4j.sh #neo4j
