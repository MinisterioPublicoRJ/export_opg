#!/bin/sh
spark2-submit \
    --executor-cores 1 \
    --num-executors 216 \
    --executor-memory 8g \
    --conf spark.yarn.executor.memoryOverhead=6g \
    --py-files src/opg_utils.py,src/timer.py src/generate_connections_mgp.py  2>> error.log