#!/bin/bash -e

PARALLELISM=500
export HADOOP_CONF_DIR=/hadoop-conf
export SPARK_HOME=/local-spark

spark-submit \
        --conf "spark.speculation=true" \
        --conf "spark.speculation.interval=5000ms" \
        --conf "spark.speculation.multiplier=5" \
        --conf "spark.speculation.quantile=0.90" \
        --conf "spark.dynamicAllocation.maxExecutors=1500" \
        --deploy-mode cluster \
        --class de.webis.copycat.app.CreateDocumentRepresentations \
        --conf spark.default.parallelism=${PARALLELISM}\
        --num-executors ${PARALLELISM}\
        --driver-memory 15G\
        target/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}
