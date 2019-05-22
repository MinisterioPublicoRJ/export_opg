#!/bin/sh
spark2-submit --py-files src/opg_output_utils.py,src/timer.py,packages/*.whl \
    --num-executors 248 \
    --executor-cores 2 \
    --executor-memory 8g  \
    src/generate_outputs.py 2>> error.log