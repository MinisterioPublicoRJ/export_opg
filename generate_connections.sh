#!/bin/sh
spark2-submit --executor-memory 8g --py-files src/opg_utils.py,src/timer.py src/generate_connections.py  2>> error.log
