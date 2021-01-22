#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


java -cp ${DIR}/../../../target/cikm20-duplicates-1.0-SNAPSHOT-jar-with-dependencies.jar \
	de.webis.cikm20_duplicates.app.DeduplicateTrecRunFile \
	${@}

