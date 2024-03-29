#!/bin/bash -e

spark-submit \
	--conf "spark.speculation=true" \
	--conf "spark.speculation.interval=5000ms" \
	--conf "spark.speculation.multiplier=5" \
	--conf "spark.speculation.quantile=0.90" \
	--deploy-mode cluster \
	--class ${1} \
        --executor-cores 1\
	--executor-memory 2G\
	--driver-memory 15G\
	--driver-cores 3\
	target/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar
