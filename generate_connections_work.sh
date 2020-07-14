#!/bin/sh
spark2-submit --master yarn --deploy-mode cluster \
    --conf "spark.kryoserializer.buffer.max=1024m" \
    --num-executors 20 \
    --executor-cores 5 \
    --driver-memory 5g \
    --executor-memory 15g \
    --conf spark.locality.wait=0 \
    --conf spark.shuffle.io.numConnectionsPerPeer=5 \
    --conf spark.speculation=true \
    --conf spark.yarn.queue="root.exportopg" \
    --conf spark.yarn.executor.memoryOverhead=6g \
    --conf spark.network.timeout=1200s \
    --py-files src/opg_utils.py,src/timer.py,src/context.py,src/base.py \
    src/generate_connections_work.py  2>> error.log