#!/bin/sh
spark2-submit \
    --executor-cores 1 \
    --py-files src/opg_utils.py,src/timer.py,src/context.py \
    src/generate_connections_embarcacao.py 2>> error.log
