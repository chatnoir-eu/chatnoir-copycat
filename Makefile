create-source-docs:
	./src/main/bash/run-spark-job.sh de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments

install:
	./mvnw clean install

