#!/bin/bash

OUT_DIR=sigir21/enrichment-cw09-cw12-pairs/part-${1}
INPUT_DIR=/corpora/corpus-copycat/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-near-duplicates-without-exact-duplicates-csv-distinct/*${1}

echo "IN=${INPUT_DIR}  OUT=${OUT_DIR}"
./src/main/bash/run-in-docker-container-with-spark.sh hdfs dfs -rm -r -f ${OUT_DIR} && \
	./src/main/bash/enrich-near-duplicates-with-s3-scores.sh \
		--input ${INPUT_DIR} \
		--output ${OUT_DIR} \
		--inputFormat csv
