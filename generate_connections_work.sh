#!/bin/sh
spark2-submit \
    --num-executors 248 \
    --executor-cores 2 \
    --executor-memory 8g \
    --py-files src/opg_utils.py,src/timer.py,src/context.py \
    src/generate_connections_work.py  2>> error.log