#!/bin/bash -e

if [ $# -ne 1 ]
then
   echo "$0 dataset-name"
   exit 1
fi

spark-submit \
	--class de.webis.copycat_spark.app.FullS3Deduplication \
	--deploy-mode cluster \
	--executor-cores 5\
	--executor-memory 25G\
	--driver-memory 25G\
	/mnt/ceph/storage/data-in-progress/data-research/arguana/args/args-near-duplicates-mapper/resources/copycat-spark-1.0-SNAPSHOT-jar-with-dependencies.jar \
	--input /mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/2022-task2/data-cleaning/${1}.jsonl \
	--eightGramIndex touche22-deduplication-task-2/8-gram-index-${1} \
	--s3Score touche22-deduplication-task-2/s3-scores-${1}

