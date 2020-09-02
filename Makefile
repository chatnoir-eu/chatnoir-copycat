new-doc-representation-on-small-sample: install
	hdfs dfs -rm -r -f s3a://corpus-copycat/document-representations/small-sample && \
	./src/main/bash/new-document-representation-spark.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2020-16/crawl-data/CC-MAIN-2020-16/segments/1585371896913.98/warc/CC-MAIN-20200410110538-20200410141038-0055*.warc.gz \
		--output s3a://corpus-copycat/document-representations/small-sample

cc2020-16-test-web-graph: install
	hdfs dfs -rm -r -f web-archive-analysis/cc2020-16-test
	./src/main/bash/run-web-graph-spark-job.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2020-16/crawl-data/CC-MAIN-2020-16/segments/1585371896913.98/warc/CC-MAIN-20200410110538-20200410141038-00559.warc.gz \
		--output web-archive-analysis/cc2020-16-test

clueweb09-document-representations: install
	hdfs dfs -rm -r -f cikm2020/camera-ready/cw09
	./src/main/bash/run-document-representation-spark-job.sh \
		--inputFormat CLUEWEB09 \
		--input s3a://corpus-clueweb09/parts/*/*/*.warc.gz \
		--output cikm2020/camera-ready/cw09

clueweb12-document-representations: install
	hdfs dfs -rm -r -f cikm2020/camera-ready/cw12
	./src/main/bash/run-document-representation-spark-job.sh \
		--inputFormat CLUEWEB12 \
		--input s3a://corpus-clueweb12/parts/*/*/*/*.warc.gz \
		--output cikm2020/camera-ready/cw12

common-crawl15-document-representations: install
	hdfs dfs -rm -r -f cikm2020/camera-ready/cc15
	./src/main/bash/run-document-representation-spark-job.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2015-11/*/*/*.warc.gz \
		--output cikm2020/camera-ready/cc15

common-crawl17-document-representations: install
	hdfs dfs -rm -r -f cikm2020/camera-ready/cc17
	./src/main/bash/run-document-representation-spark-job.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2017-04/*/*/*.warc.gz \
		--output cikm2020/camera-ready/cc17

label-data-daniel:
	./src/main/bash/label-data-canonical-edges-daniel.sh

repartition-parts: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.util.SparkRepartitionParts

report-feature-sets: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures

prepare-precision-experiments: install
	./src/main/bash/run-spark-job-low-resources-akbnq.sh de.webis.cikm20_duplicates.spark.eval.SparkCalculatePrecisionInCanonicalLinkGraph

report-precision-experiments: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkCalculatePrecisionInCanonicalLinkGraph

sample-canonical-link-graph-edges: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkSampleS3EdgesPerBin

corpus-analysis: install
	./src/main/bash/run-spark-job-low-resources-akbnq-new.sh de.webis.cikm20_duplicates.spark.eval.SparkCorpusAnalysis

distinct-deduplication-pairs: install
	./src/main/bash/run-spark-job-low-resources-akbnq-new.sh de.webis.cikm20_duplicates.spark.SparkMakeDeduplicatedPairsUnique

create-ids-to-remove: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateIdsToRemove

report-short-documents: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeShortDocuments

analize-tmp: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeCanonicalLinkGraph

report-document-lengths: install
	./src/main/bash/run-low-resource-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeDocumentLength
	
s3-score-per-hamming: install
	./src/main/bash/run-low-resource-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeAverageS3ScorePerHammingDistance

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
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction

relevance-transfer: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAggregateKnowledgeTransferBetweenCrawls

relevance-transfer-ids: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkCreateTargetDocumentsForRelevanceTransfer

crawl-containment: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkIdentifyDocumentsInTargetCrawl

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
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkDeduplicateCandidates

deduplication-task-sizes: install
	./src/main/bash/run-low-resource-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeDeduplicationTaskSizes

create-candidates: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates

create-url-candidates: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateCanonicalLinkDeduplicationTasks

create-source-docs: install
	./src/main/bash/run-low-resource-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments

repartition-source-docs-cc15: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.util.SparkRepartitionCommonCrawl2015SourceDocuments

repartition-source-docs-cc17: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.util.SparkRepartitionCommonCrawl2017SourceDocuments

install:
	./mvnw clean install

label-data-maik:
	./src/main/bash/label-data-maik.sh

canonical-edges.pdf: src/main/python/plot.py
	python3 src/main/python/plot.py

plots: canonical-edges.pdf
