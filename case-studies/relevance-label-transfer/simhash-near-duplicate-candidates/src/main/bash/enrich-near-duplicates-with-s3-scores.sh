#!/bin/bash -e

export HADOOP_CONF_DIR=/hadoop-conf
export SPARK_HOME=/local-spark

spark-submit \
        --conf "spark.speculation=true" \
        --conf "spark.speculation.interval=5000ms" \
        --conf "spark.speculation.multiplier=5" \
        --conf "spark.speculation.quantile=0.90" \
        --conf "spark.dynamicAllocation.maxExecutors=450" \
        --deploy-mode cluster \
        --class de.webis.copycat.app.EnrichSimHashNearDuplicatesWithS3Similarity \
        --conf spark.default.parallelism=50000\
        --executor-cores 1\
        --num-executors 300\
        --driver-memory 5G\
        target/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

