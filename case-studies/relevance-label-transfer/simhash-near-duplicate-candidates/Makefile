sigir2021-enrich-documents-with-s3-score: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.app.EnrichPairsOfDocumentsWithS3SCore

create-documents-for-daniel: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.app.InjectRawDocuments

common-crawl-small-sample-document-representations: install
	hdfs dfs -rm -r -f corpus-copycat/document-representations/small-sample && \
	./src/main/bash/new-document-representation-spark.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2020-16/crawl-data/CC-MAIN-2020-16/segments/1585371896913.98/warc/CC-MAIN-20200410110538-20200410141038-005{1,2,3,4,5}*.warc.gz \
		--output corpus-copycat/document-representations/small-sample

cc2020-16-test-web-graph: install
	hdfs dfs -rm -r -f web-archive-analysis/cc2020-16-test
	./src/main/bash/run-web-graph-spark-job.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2020-16/crawl-data/CC-MAIN-2020-16/segments/1585371896913.98/warc/CC-MAIN-20200410110538-20200410141038-00559.warc.gz \
		--output web-archive-analysis/cc2020-16-test

clueweb09-document-representations: install
	hdfs dfs -rm -r -f sigir2021/cw09 && \
	./src/main/bash/new-document-representation-spark.sh \
		--inputFormat CLUEWEB09 \
		--input s3a://corpus-clueweb09/parts/*/*/*.warc.gz \
		--output sigir2021/cw09 && \
	hdfs dfs -rm -r -f sigir2021/cw09-repartitioned && \
	src/main/bash/repartition.sh \
		--input sigir2021/cw09 \
		--output sigir2021/cw09-repartitioned \
		--partitions 10000

clueweb09-main-content-document-representations: install
	hdfs dfs -rm -r -f sigir2021/cw09-main-content-extraction && \
	./src/main/bash/new-document-representation-spark.sh \
		--inputFormat CLUEWEB09 \
		--input s3a://corpus-clueweb09/parts/*/*/*.warc.gz \
		--mainContentExtraction true \
		--output sigir2021/cw09-main-content-extraction

clueweb12-document-representations: install
	hdfs dfs -rm -r -f sigir2021/cw12 && \
	./src/main/bash/new-document-representation-spark.sh \
		--inputFormat CLUEWEB12 \
		--input s3a://corpus-clueweb12/parts/*/*/*/*.warc.gz \
		--output sigir2021/cw12 && \
	hdfs dfs -rm -r -f sigir2021/cw12-repartitioned && \
	src/main/bash/repartition.sh \
		--input sigir2021/cw12 \
		--output sigir2021/cw12-repartitioned \
		--partitions 10000

clueweb12-main-content-document-representations: install
	hdfs dfs -rm -r -f sigir2021/cw12-main-content-extraction && \
	./src/main/bash/new-document-representation-spark.sh \
		--inputFormat CLUEWEB12 \
		--input s3a://corpus-clueweb12/parts/*/*/*/*.warc.gz \
		--mainContentExtraction true \
		--output sigir2021/cw12-main-content-extraction

common-crawl15-document-representations: install
	hdfs dfs -rm -r -f sigir2021/cc-2015-11 && \
	./src/main/bash/cc15-document-representations.sh 0 && \
	./src/main/bash/cc15-document-representations.sh 1 && \
	./src/main/bash/cc15-document-representations.sh 2 && \
	./src/main/bash/cc15-document-representations.sh 3 && \
	./src/main/bash/cc15-document-representations.sh 4 && \
	./src/main/bash/cc15-document-representations.sh 5 && \
	./src/main/bash/cc15-document-representations.sh 6 && \
	./src/main/bash/cc15-document-representations.sh 7 && \
	./src/main/bash/cc15-document-representations.sh 8 && \
	./src/main/bash/cc15-document-representations.sh 9 && \
	hdfs dfs -rm -r -f sigir2021/cc-2015-11-repartitioned && \
	src/main/bash/repartition.sh \
		--input sigir2021/cc-2015-11/*/ \
		--output sigir2021/cc-2015-11-repartitioned \
		--partitions 10000

common-crawl17-document-representations: install
	hdfs dfs -rm -r -f sigir2021/cc-2017-04 && \
	./src/main/bash/cc17-document-representations.sh 0 && \
	./src/main/bash/cc17-document-representations.sh 1 && \
	./src/main/bash/cc17-document-representations.sh 2 && \
	./src/main/bash/cc17-document-representations.sh 3 && \
	./src/main/bash/cc17-document-representations.sh 4 && \
	./src/main/bash/cc17-document-representations.sh 5 && \
	./src/main/bash/cc17-document-representations.sh 6 && \
	./src/main/bash/cc17-document-representations.sh 7 && \
	./src/main/bash/cc17-document-representations.sh 8 && \
	./src/main/bash/cc17-document-representations.sh 9 && \
	hdfs dfs -rm -r -f sigir2021/cc-2017-04-repartitioned && \
	src/main/bash/repartition.sh \
		--input sigir2021/cc-2017-04/*/ \
		--output sigir2021/cc-2017-04-repartitioned \
		--partitions 10000

report-mime-types: install
	hdfs dfs -rm -r -f sigir2021/mime-types/cw09 && \
	./src/main/bash/report-mime-types.sh \
		--inputFormat CLUEWEB09 \
		--input s3a://corpus-clueweb09/parts/*/*/*.warc.gz \
		--output sigir2021/mime-types/cw09 && \
	hdfs dfs -rm -r -f sigir2021/mime-types/cw12 && \
	./src/main/bash/report-mime-types.sh \
		--inputFormat CLUEWEB12 \
		--input s3a://corpus-clueweb12/parts/*/*/*/*.warc.gz \
		--output sigir2021/mime-types/cw12 && \
	hdfs dfs -rm -r -f sigir2021/mime-types/cc-2015-11 && \
	./src/main/bash/report-mime-types.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2015-11/*/*/*.warc.gz \
		--output sigir2021/mime-types/cc-2015-11 && \
	hdfs dfs -rm -r -f sigir2021/mime-types/cc-2017-04 && \
	./src/main/bash/report-mime-types.sh \
		--inputFormat COMMON_CRAWL \
		--input s3a://corpus-commoncrawl-main-2017-04/*/*/*.warc.gz  \
		--output sigir2021/mime-types/cc-2017-04

arc-url-extraction: install
	./src/main/bash/part-arc-url-extraction.sh 0 && \
	./src/main/bash/part-arc-url-extraction.sh 1 && \
	./src/main/bash/part-arc-url-extraction.sh 2 && \
	./src/main/bash/part-arc-url-extraction.sh 3 && \
	./src/main/bash/part-arc-url-extraction.sh 4 && \
	./src/main/bash/part-arc-url-extraction.sh 5 && \
	./src/main/bash/part-arc-url-extraction.sh 6 && \
	./src/main/bash/part-arc-url-extraction.sh 7 && \
	./src/main/bash/part-arc-url-extraction.sh 8 && \
	./src/main/bash/part-arc-url-extraction.sh 9

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

distinct-deduplication-pairs-onegramms: install 
	./src/main/bash/distinct-near-duplicate-pairs.sh \
		-i sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/near-duplicates \
		-o sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-near-duplicates

deduplication-pairs-between-corpora-onegramms: install
	hdfs dfs -rm -r -f  sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-near-duplicates-between-corpora && \
	./src/main/bash/deduplication-pairs-between-corpora.sh \
		-i sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-near-duplicates \
		-o sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-near-duplicates-between-corpora

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

deduplicate-cw09: install
	src/main/bash/deduplicate.sh \
		--input sigir2021/cw09-deduplication/min-length-10-near-duplicate-tasks \
		--output sigir2021/cw09-deduplication/min-length-10-near-duplicates

deduplicate-cw12: install
	src/main/bash/deduplicate.sh \
		--input sigir2021/cw12-deduplication/min-length-10-near-duplicate-tasks \
		--output sigir2021/cw12-deduplication/min-length-10-near-duplicates

deduplicate-cw09-cw12-cc15: install
	src/main/bash/deduplicate.sh \
		--input sigir2021/cw09-cw12-cc15-deduplication/min-length-10-near-duplicate-tasks \
		--output sigir2021/cw09-cw12-cc15-deduplication/min-length-10-near-duplicates

deduplicate-cc15: install
	src/main/bash/deduplicate.sh \
		--input sigir2021/cc-2015-11-deduplication/min-length-10-near-duplicate-tasks \
		--output sigir2021/cc-2015-11-deduplication/min-length-10-near-duplicates

deduplicate-cc17: install
	src/main/bash/deduplicate.sh \
		--input sigir2021/cc-2017-04-deduplication/min-length-10-near-duplicate-tasks \
		--output sigir2021/cc-2017-04-deduplication/min-length-10-near-duplicates

sample-near-duplicates-cw09: install
	hdfs dfs -rm -r -f sigir2021/cw09-deduplication/sample-near-duplicates-min-length-10.jsonl && \
	src/main/bash/sample-near-duplicates.sh \
		--input sigir2021/cw09-deduplication/min-length-10 \
		--num 10000 \
		--output sigir2021/cw09-deduplication/sample-near-duplicates-min-length-10.jsonl \
		--uuidIndex cw09 \
		--uuidPrefix clueweb09

enrich-near-duplicate-pairs-with-judged-documents: install
	hdfs dfs -rm -r -f sigir2021/cw09-deduplication/tmp-cw09-enriched-near-duplicate-pairs && \
	src/main/bash/enrich-near-duplicates-with-s3-scores.sh \
		--input sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-near-duplicates-between-corpora/with-judgments-in-web-track \
		--output sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/relevance-transfer-near-duplicates

exact-duplicates-between-corpora-for-relevance-transfer: install
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 0 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 1 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 2 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 3 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 4 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 5 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 6 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 7 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 8 && \
	./src/main/bash/part-enrich-near-duplicates-with-s3-scores.sh 9

sample-near-duplicates-cw12: install
	hdfs dfs -rm -r -f sigir2021/cw12-deduplication/sample-near-duplicates-min-length-10.jsonl && \
	src/main/bash/sample-near-duplicates.sh \
		--input sigir2021/cw12-deduplication/min-length-10 \
		--num 10000 \
		--output sigir2021/cw12-deduplication/sample-near-duplicates-min-length-10.jsonl \
		--uuidIndex cw12 \
		--uuidPrefix clueweb12

sample-near-duplicates-cc17: install
	hdfs dfs -rm -r -f sigir2021/cc-2017-04-deduplication/sample-near-duplicates-min-length-10.jsonl && \
	src/main/bash/sample-near-duplicates.sh \
		--input sigir2021/cc-2017-04-deduplication/min-length-10 \
		--num 10000 \
		--output sigir2021/cc-2017-04-deduplication/sample-near-duplicates-min-length-10.jsonl \
		--uuidIndex cc1704 \
		--uuidPrefix commoncrawl

sample-near-duplicates-cc15: install
	hdfs dfs -rm -r -f sigir2021/cc-2015-11-deduplication/sample-near-duplicates-min-length-10.jsonl && \
	src/main/bash/sample-near-duplicates.sh \
		--input sigir2021/cc-2015-11-deduplication/min-length-10 \
		--num 10000 \
		--output sigir2021/cc-2015-11-deduplication/sample-near-duplicates-min-length-10.jsonl \
		--uuidIndex cc1511 \
		--uuidPrefix commoncrawl

deduplication-task-sizes: install
	./src/main/bash/run-low-resource-spark-job.sh de.webis.cikm20_duplicates.spark.eval.SparkAnalyzeDeduplicationTaskSizes

create-deduplication-candidates-cw09: install
	src/main/bash/create-deduplication-candidates.sh \
		--input sigir2021/cw09-repartitioned/ \
		--output sigir2021/cw09-deduplication/ \
		--minimumDocumentLength 10

create-deduplication-candidates-cw12: install
	src/main/bash/create-deduplication-candidates.sh \
		--input sigir2021/cw12-repartitioned/ \
		--output sigir2021/cw12-deduplication/ \
		--minimumDocumentLength 10

create-deduplication-candidates-cc15: install
	src/main/bash/create-deduplication-candidates.sh \
		--input sigir2021/cc-2015-11-repartitioned/ \
		--output sigir2021/cc-2015-11-deduplication/ \
		--minimumDocumentLength 10

create-deduplication-candidates-cw09-cw12-cc15: install
	hdfs dfs -rm -r -f sigir2021/cw09-cw12-cc15-deduplication &&\
	src/main/bash/create-deduplication-candidates.sh \
		--input sigir2021/{cw09,cw12,cc-2015-11}-repartitioned/ \
		--output sigir2021/cw09-cw12-cc15-deduplication/ \
		--minimumDocumentLength 10

create-deduplication-candidates-cc17: install
	src/main/bash/create-deduplication-candidates.sh \
		--input sigir2021/cc-2017-04-repartitioned/ \
		--output sigir2021/cc-2017-04-deduplication/ \
		--minimumDocumentLength 10

create-url-candidates: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateCanonicalLinkDeduplicationTasks

create-source-docs: install
	./src/main/bash/run-low-resource-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments

repartition-source-docs-cc15: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.util.SparkRepartitionCommonCrawl2015SourceDocuments

repartition-source-docs-cc17: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.util.SparkRepartitionCommonCrawl2017SourceDocuments

install:
	./mvnw clean install 2> /dev/null

label-data-maik:
	./src/main/bash/label-data-maik.sh

canonical-edges.pdf: src/main/python/plot.py
	python3 src/main/python/plot.py

