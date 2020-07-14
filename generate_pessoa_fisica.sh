#!/bin/sh
export PYTHONIOENCODING=utf8

spark2-submit --master yarn --deploy-mode cluster \
    --conf "spark.kryoserializer.buffer.max=1024m" \
    --num-executors 12 \
    --executor-cores 5 \
    --driver-memory 5g \
    --executor-memory 15g \
    --conf spark.locality.wait=0 \
    --conf spark.shuffle.io.numConnectionsPerPeer=5 \
    --conf spark.speculation=true \
    --conf spark.yarn.queue="root.exportopg" \
    --conf spark.yarn.executor.memoryOverhead=6g \
    --conf spark.executor.memoryOverhead=6g \
    --conf spark.default.parallelism=200 \
    --conf spark.sql.shuffle.partitions=100 \
    --conf spark.network.timeout=360 \
    --py-files src/opg_utils.py,src/timer.py,src/context.py,src/base.py,packages/Unidecode-1.1.1-py2.py3-none-any.whl \
    src/generate_pessoa_fisica.py 2> error.log
