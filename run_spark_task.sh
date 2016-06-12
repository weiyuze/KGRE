#!/bin/sh

#INPUT="./data/test.txt"

hadoop fs -rm -r /weiyuze/kgoutput2

logtime=`date +%Y%m%d%H%M%S`
EXECUTORCORES=6
EXECUTORMEM="10G"
PARALLELISM=300
echo "--executor-cores:" >> ./log/log.${logtime}
echo ${EXECUTORCORES} >> ./log/log.${logtime}
echo "--executor-memory:" >> ./log/log.${logtime}
echo ${EXECUTORMEM} >> ./log/log.${logtime}
echo "--parallelism:" >> ./log/log.${logtime}
echo ${PARALLELISM} >> ./log/log.${logtime}
echo "" >> ./log/log.${logtime}

spark-submit \
--master spark://10.2.28.65:7077 \
--executor-cores ${EXECUTORCORES} \
--executor-memory ${EXECUTORMEM} \
./src/reasoner_t.py \
"./data/schemaFileLists.txt" \
"./data/instanceFileLists.txt" \
"/weiyuze/kgoutput2" \
"./data/rules.txt" \
${PARALLELISM} \
>> ./log/log.${logtime}

exit 0
