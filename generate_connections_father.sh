#!/bin/sh
spark2-submit \
    --num-executors 200 \
    --executor-cores 1 \
    --executor-memory 8g \
    --py-files src/opg_utils.py,src/timer.py src/generate_connections_father.py  2>> error.log