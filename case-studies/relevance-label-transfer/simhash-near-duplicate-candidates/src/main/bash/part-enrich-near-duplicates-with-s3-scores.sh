#!/bin/bash -e

./src/main/bash/enrich-near-duplicates-with-s3-scores.sh \
	--input sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/distinct-exact-duplicates-between-corpora-for-relevance-transfer-repartitioned/*${1}.bz2 \
	--output sigir2021/cw09-cw12-cc15-onegram-deduplication/min-length-10-/64BitK3SimHashOneGramms/relevance-transfer-exact-duplicates/part-${1}

