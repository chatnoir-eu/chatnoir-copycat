#!/bin/bash -e

./src/main/bash/ids-to-remove.sh \
	--exactDuplicateInputs s3a://corpus-copycat/exact-duplicates/{cw09-cw12-cc-2015-11,cc-2015-11,cw09,cw12}/ s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/ \
	--nearDuplicateInputs s3a://corpus-copycat/near-duplicates/cw09-cw12-cc-2015-11/ s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/ \
	--output sigir21/docs-to-remove/cw09-cw12-cc15/intermediate/simhash-3-5-grams-docs-to-remove sigir21/docs-to-remove/cw09-cw12-cc15/intermediate/url-groups-docs-to-remove sigir21/docs-to-remove/cw09-cw12-cc15/docs-to-remove \
	--keepIds ALL

