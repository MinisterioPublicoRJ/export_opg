#!/bin/sh
export PYTHONIOENCODING=utf8

spark2-submit --master yarn --deploy-mode cluster \
        --py-files src/opg_utils.py,src/timer.py,src/context.py,src/base.py \
	--num-executors 20 \
        --executor-cores 5 \
        --driver-memory 5g \
        --executor-memory 12g \
	--conf spark.locality.wait=0 \
	--conf spark.shuffle.io.numConnectionsPerPeer=5 \
	--conf spark.speculation=true \
        --conf spark.default.parallelism=100 \
        --conf spark.sql.shuffle.partitions=100 \
	--conf spark.yarn.queue="root.exportopg" \
        --conf spark.yarn.executor.memoryOverhead=6g \
        src/generate_uuids.py 2>> error.log
