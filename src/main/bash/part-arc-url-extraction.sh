#!/bin/bash -e

hdfs dfs -rm -r -f internet-archive/analysis-in-progress/cc-2008-urls/part-${1}

./src/main/bash/arc-url-extraction.sh \
	--input s3a://corpus-commoncrawl-main-2008/*/*/*/*/*/*${1}_0.arc.gz \
	--output internet-archive/analysis-in-progress/cc-2008-urls/part-${1}

