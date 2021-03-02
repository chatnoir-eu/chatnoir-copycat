#!/bin/bash -e

export HADOOP_CONF_DIR=/home/kibi9872/web-search-sigir2021-resource-paper-code/hadoop-conf
export SPARK_HOME=/home/kibi9872/web-search-sigir2021-resource-paper-code/local-spark

/home/kibi9872/web-search-sigir2021-resource-paper-code/local-spark/bin/spark-submit \
        --conf "spark.speculation=true" \
        --conf "spark.speculation.interval=5000ms" \
        --conf "spark.speculation.multiplier=5" \
        --conf "spark.speculation.quantile=0.90" \
        --conf "spark.dynamicAllocation.maxExecutors=100" \
        --deploy-mode cluster \
        --class de.webis.sigir2021.spark.TransformJudgedDocumentsToNearDuplicateListsForCC15 \
        --conf spark.default.parallelism=100\
        --num-executors 100\
        --driver-memory 25G\
        --executor-memory 15G\
        target/sigir21-relevance-transfer-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

