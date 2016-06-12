#!/bin/sh

#INPUT="./data/test.txt"

hadoop fs -rm -r /weiyuze/kgoutput_schema

logtime=`date +%Y%m%d%H%M%S`

spark-submit \
--master spark://10.2.28.65:7077 \
--executor-cores 6 \
--executor-memory 10g \
./src/schemaReasoner.py \
"./data/schemaFileLists.txt" \
"/weiyuze/kgoutput_schema" \
"./data/rules.txt" \
> ./log/log_schema.${logtime}

exit 0
