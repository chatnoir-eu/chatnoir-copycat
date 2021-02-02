#!/bin/bash -e

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"


java -jar ${DIR}/../../../copycat-cli/target/copycat-cli-1.0-SNAPSHOT-jar-with-dependencies.jar \
	${@}

