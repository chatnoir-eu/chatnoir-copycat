#!/bin/bash -e

PARALLELISM=70

spark-submit \
	--deploy-mode cluster \
	--class ${1} \
	--conf spark.default.parallelism=${PARALLELISM}\
	--num-executors ${PARALLELISM}\
	--executor-memory 15G\
	--driver-memory 15G\
	target/cikm20-duplicates-1.0-SNAPSHOT-jar-with-dependencies.jar

