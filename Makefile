create-relevance-transfer-dataset: install
	hdfs dfs -rm -r -f cikm2020/relevance-transfer-pairs.jsonl && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction

count-edges: install
	hdfs dfs -rm -r -f cikm2020/near-duplicate-graph/edge-count-cw09-cw12 && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCountEdgeLabels

combine-intermediate-results: install
	hdfs dfs -rm -r -f cikm2020/results/test-01 && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCombineIntermediateResults

deduplicate: install
	hdfs dfs -rm -r -f cikm2020/deduplication/near-duplicates/cw09-cw12 && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkDeduplicateCandidates

create-candidates: install
	hdfs dfs -rm -r -f cikm2020/exact-duplicates-simhash-cw09-cw12 &&\
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates

create-source-docs: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments

install:
	./mvnw clean install

