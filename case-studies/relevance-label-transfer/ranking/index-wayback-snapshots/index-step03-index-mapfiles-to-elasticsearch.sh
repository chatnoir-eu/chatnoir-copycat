#!/bin/bash -e

rm -Rf chatnoir-indexer
git clone --single-branch --branch clueweb09-in-wayback12 git@webis.uni-weimar.de:code-research/web-search/chatnoir/chatnoir-indexer.git chatnoir-indexer

cd chatnoir-indexer
./gradlew shadowJar
cd ..

/opt/hadoop-2.7.1/bin/hadoop jar chatnoir-indexer/build/libs/chatnoir2-indexer-*-all.jar \
    "de.webis.chatnoir2.indexer.app.ChatNoirIndexer" \
    -Des.nodes=betaweb015,betaweb016,betaweb017,betaweb018,betaweb019 \
    -Des.batch.size.entries=7000 \
    -Des.batch.size.bytes=10mb \
    -Dmapreduce.job.split.metainfo.maxsize=-1 \
    -Dmapred.map.tasks=5 \
    -Dmapred.reduce.tasks=5 \
    -uuid-prefix "clueweb09-in-wayback12" \
    -sequence-files "sigir2021/clueweb09-in-wayback12/segment-00000/" \
    -batches 1 \
    -index "clueweb09-in-wayback12"

