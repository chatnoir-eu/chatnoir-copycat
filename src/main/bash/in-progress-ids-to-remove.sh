#!/bin/bash

./src/main/bash/ids-to-remove.sh \
	--exactDuplicateInputs sigir21/simhash-3-5-grams-exact-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/ \
	--nearDuplicateInputs sigir21/simhash-3-5-grams-near-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/ \
	--output sigir21/docs-to-remove/cw09/intermediate/simhash-3-5-grams-docs-to-remove sigir21/docs-to-remove/cw09/intermediate/url-groups-docs-to-remove sigir21/docs-to-remove/cw09/docs-to-remove \
	--keepIds CW09

./src/main/bash/ids-to-remove.sh \
	--exactDuplicateInputs sigir21/simhash-3-5-grams-exact-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/ \
	--nearDuplicateInputs sigir21/simhash-3-5-grams-near-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/ \
	--output sigir21/docs-to-remove/cw12/intermediate/simhash-3-5-grams-docs-to-remove sigir21/docs-to-remove/cw12/intermediate/url-groups-docs-to-remove sigir21/docs-to-remove/cw12/docs-to-remove \
	--keepIds CW12

./src/main/bash/ids-to-remove.sh \
	--exactDuplicateInputs sigir21/simhash-3-5-grams-exact-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/ \
	--nearDuplicateInputs sigir21/simhash-3-5-grams-near-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/ \
	--output sigir21/docs-to-remove/cc15/intermediate/simhash-3-5-grams-docs-to-remove sigir21/docs-to-remove/cc15/intermediate/url-groups-docs-to-remove sigir21/docs-to-remove/cc15/docs-to-remove \
	--keepIds CC

./src/main/bash/ids-to-remove.sh \
	--exactDuplicateInputs sigir21/simhash-3-5-grams-exact-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/ \
	--nearDuplicateInputs sigir21/simhash-3-5-grams-near-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/ \
	--output sigir21/docs-to-remove/cw09b/intermediate/simhash-3-5-grams-docs-to-remove sigir21/docs-to-remove/cw09b/intermediate/url-groups-docs-to-remove sigir21/docs-to-remove/cw09b/docs-to-remove \
	--keepIds CW09b

./src/main/bash/ids-to-remove.sh \
	--exactDuplicateInputs sigir21/simhash-3-5-grams-exact-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/simhash-one-grams-cw09-cw12-cc15-exact-duplicates/url-simhash-one-grams-cw09-cw12-cc15-exact-duplicates/ \
	--nearDuplicateInputs sigir21/simhash-3-5-grams-near-duplicates-cw09-cw12-cc15 s3a://corpus-copycat/canonical-url-groups/near-duplicates-simhash-one-grams-cw09-cw12-cc15/ \
	--output sigir21/docs-to-remove/cw12b13/intermediate/simhash-3-5-grams-docs-to-remove sigir21/docs-to-remove/cw12b13/intermediate/url-groups-docs-to-remove sigir21/docs-to-remove/cw12b13/docs-to-remove \
	--keepIds CW12b

