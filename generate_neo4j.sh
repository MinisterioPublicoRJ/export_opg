#!/bin/sh
spark2-submit --py-files src/opg_output_utils.py,src/opg_utils.py,src/timer.py,src/neo4j_output_utils.py,packages/*.whl,packages/*.egg \
    --num-executors 248 \
    --executor-cores 2 \
    --executor-memory 8g  \
    src/generate_neo4j_outputs.py 2>> error.log
