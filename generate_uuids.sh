#!/bin/sh
export PYTHONIOENCODING=utf8

spark2-submit --py-files src/opg_utils.py,src/timer.py,src/context.py \
        --num-executors 300 \
        --executor-cores 1 \
        --executor-memory 4g  \
        --conf spark.yarn.queue="root.mpmapas" \
        --conf spark.yarn.executor.memoryOverhead=6g \
        src/generate_uuids.py 2>> error.log
