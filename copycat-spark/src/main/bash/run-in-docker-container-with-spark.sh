#!/bin/bash -e

if [[ ! -f conf/hadoop/core-site.xml ]]
then
    echo "The configuration conf/hadoop/core-site.xml is missing. This file can not be stored in git, because it contains credentials for accessing S3. You can find an example core-site.xml in https://git.webis.de/code-admin/jupyterhub-helm/-/blob/master/conf/hadoop/core-site.xml"
    exit 1
fi

docker run --rm -ti \
	-v ${PWD}/target:/target \
	-v ${PWD}/conf/hadoop:/etc/hadoop \
	-v ${PWD}/conf/spark:/etc/spark \
	-e HADOOP_USER_NAME=${USER} \
	-e HADOOP_CONF_DIR=/etc/hadoop \
	-e SPARK_CONF_DIR=/etc/spark \
	-e JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/ \
	webis/sparknotebook ${@}

