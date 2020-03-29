create-candidates: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates

create-source-docs: install
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments

install:
	./mvnw clean install

