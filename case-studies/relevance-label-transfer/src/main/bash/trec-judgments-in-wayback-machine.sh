#!/bin/bash -e

java -Dlog4j.configurationFile=src/main/resources/log4j2.xml \
	-cp target/sigir21-relevance-transfer-1.0-SNAPSHOT-jar-with-dependencies.jar \
	de.webis.sigir2021.App ${@}

