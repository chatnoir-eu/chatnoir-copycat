label-data-daniel:
	./src/main/bash/label-data-canonical-edges-daniel.sh

sample-canonical-link-graph-edges: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkSampleS3EdgesPerBin

analyze-bla: install
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cw12-s3-edge-aggregations && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cw09-s3-edge-aggregations && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cc-2015-11-s3-edge-aggregations && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeCanonicalLinkGraph

create-canonical-link-graph-edges: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCalculateCanonicalLinkGraphEdgeLabels

analyze-canonical-link-graph: install
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cw12-duplicate-group-counts && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cw12-duplicate-group-counts-per-domain && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cw09-duplicate-group-counts && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cw09-duplicate-group-counts-per-domain && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cc-2015-11-duplicate-group-counts && \
	hdfs dfs -rm -R -f cikm2020/canonical-link-graph/cc-2015-11-duplicate-group-counts-per-domain && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeCanonicalLinkGraph

create-canonical-link-graph: install
	hdfs dfs -rm -r -f cikm2020/canonical-link-graph/cw09 && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction

enrich-relevance-transfer-dataset: install
	hdfs dfs -rm -r -f cikm2020/enriched-relevance-transfer-pairs.jsonl && \
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs

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
	hdfs dfs -rm -r -f cikm2020/document-fingerprints-commoncrawl-main-2015-11 &&\
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments

install:
	./mvnw clean install

label-data-maik:
	./src/main/bash/label-data-maik.sh

