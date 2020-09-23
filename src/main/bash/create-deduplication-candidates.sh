#!/bin/bash -e

PARALLELISM=100

spark-submit \
	--conf "spark.speculation=true" \
	--conf "spark.speculation.interval=5000ms" \
	--conf "spark.speculation.multiplier=5" \
	--conf "spark.speculation.quantile=0.90" \
	--deploy-mode cluster \
	--class de.webis.cikm20_duplicates.app.CreateDeduplicationCandidates \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--executor-cores 5 \
	--executor-memory 25G\
	--driver-memory 25G\
	target/cikm20-duplicates-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

