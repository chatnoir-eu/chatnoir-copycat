install:
	./mvnw clean install -f copycat-modules/interfaces/pom.xml 2> /dev/null && \
	./mvnw clean install -f copycat-modules/document-preprocessing/pom.xml 2> /dev/null && \
	./mvnw clean install -f copycat-modules/anserini-integration/pom.xml 2> /dev/null && \
	./mvnw clean install -f copycat-spark/pom.xml 2> /dev/null && \
	./mvnw clean install -f copycat-cli/pom.xml

jupyter-notebook:
	docker run -ti --rm -p 8888:8888 \
		-v ${PWD}:/workdir \
		-v /mnt/ceph/storage/data-in-progress/data-research/web-search/:/mnt/ceph/storage/data-in-progress/data-research/web-search/ \
		-w /workdir \
		capreolus:0.2.5 \
		jupyter notebook --no-browser --ip=0.0.0.0 --allow-root

