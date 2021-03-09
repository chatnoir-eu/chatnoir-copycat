#!/bin/bash -e

./src/main/bash/run-in-docker-container-with-spark.sh spark-submit \
        --conf "spark.speculation=true" \
        --conf "spark.speculation.interval=5000ms" \
        --conf "spark.speculation.multiplier=5" \
        --conf "spark.speculation.quantile=0.90" \
        --conf "spark.dynamicAllocation.maxExecutors=75" \
        --conf "spark.yarn.maxAppAttempts=1" \
        --deploy-mode cluster \
        --class de.webis.cikm20_duplicates.app.EnrichGroupsOfDocumentsWithS3Score \
        --conf spark.default.parallelism=5000\
        --executor-cores 1\
        --num-executors 50\
        --driver-memory 15G\
        --executor-memory 10G\
        /target/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

