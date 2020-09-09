#!/bin/bash -e

./src/main/bash/new-document-representation-spark.sh \
	--inputFormat COMMON_CRAWL \
	--input s3a://corpus-commoncrawl-main-2017-04/*${1}/*/*.warc.gz \
	--output ecir2021/cc-2017-04/part-${1}

