#!/bin/bash -e

PARALLELISM=100

spark-submit \
	--conf "spark.speculation=true" \
	--conf "spark.speculation.interval=5000ms" \
	--conf "spark.speculation.multiplier=5" \
	--conf "spark.speculation.quantile=0.90" \
	--deploy-mode cluster \
	--class de.webis.copycat.app.Repartition \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--driver-memory 40G\
	target/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar ${@}

