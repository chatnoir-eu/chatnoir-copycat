#!/bin/bash -e

PARALLELISM=500

./src/main/bash/run-in-docker-container-with-spark.sh spark-submit \
        --conf "spark.speculation=true" \
        --conf "spark.speculation.interval=5000ms" \
        --conf "spark.speculation.multiplier=5" \
        --conf "spark.speculation.quantile=0.90" \
        --conf "spark.dynamicAllocation.maxExecutors=1500" \
        --deploy-mode cluster \
        --class de.webis.cikm20_duplicates.app.CreateDocumentHashes \
        --conf spark.default.parallelism=${PARALLELISM}\
        --num-executors ${PARALLELISM}\
        --driver-memory 15G\
        /target/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

