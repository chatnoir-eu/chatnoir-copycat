FROM jupyter/datascience-notebook:python-3.8.6

USER root

RUN apt-get update && \
	apt-get install -y openjdk-8-jdk && \
	mkdir /copycat

COPY copycat-cli/target/copycat-cli-1.0-SNAPSHOT-jar-with-dependencies.jar /copycat/
COPY src/main/bash/copy-cat-in-docker.sh /usr/bin/copy-cat

USER jovyan
