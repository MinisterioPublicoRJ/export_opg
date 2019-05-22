#!/bin/sh
spark2-submit --conf "spark.kryoserializer.buffer.max=1024m" --conf spark.yarn.executor.memoryOverhead=2000 --conf spark.network.timeout=1200s \
    --py-files src/opg_utils.py,src/timer.py src/generate_pessoa_fisica.py 2> error.log
