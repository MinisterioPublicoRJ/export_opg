#!/bin/sh
spark2-submit --master yarn --deploy-mode cluster \
    --conf "spark.kryoserializer.buffer.max=1024m" \
    --num-executors 20 \
    --executor-cores 5 \
    --driver-memory 6g \
    --executor-memory 20g \
    --conf spark.locality.wait=0 \
    --conf spark.yarn.queue="root.exportopg" \
    --conf spark.executor.memoryOverhead=7g \
    --conf spark.default.parallelism=100 \
    --conf spark.sql.shuffle.partitions=100 \
    --conf spark.network.timeout=2000s \
    --py-files src/opg_utils.py,src/timer.py,src/context.py,src/base.py \
    src/generate_connections_mgp.py 2> error.log
