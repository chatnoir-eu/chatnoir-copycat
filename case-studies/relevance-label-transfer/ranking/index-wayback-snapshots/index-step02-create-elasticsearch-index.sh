#!/bin/bash


rm -Rf chatnoir-indexer
git clone --single-branch --branch clueweb09-in-wayback12 git@webis.uni-weimar.de:code-research/web-search/chatnoir/chatnoir-indexer.git chatnoir-indexer

./chatnoir-indexer/src/scripts/elasticsearch/templates/create-clueweb09-in-wayback12-index.sh

