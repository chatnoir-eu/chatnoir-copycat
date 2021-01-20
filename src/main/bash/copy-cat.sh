#!/bin/bash -e

java -cp target/cikm20-duplicates-1.0-SNAPSHOT-jar-with-dependencies.jar \
	de.webis.cikm20_duplicates.app.DeduplicateTrecRunFile \
	--output delete-me \
	--input input.UAMSA10d2a8 \
	--threads 15 \
	--docResolver WebisChatNoirMapfiles \
	--similarities canonicalUrl s3Score 64BitK3SimHashOneGramms 64BitK3SimHashThreeAndFiveGramms

