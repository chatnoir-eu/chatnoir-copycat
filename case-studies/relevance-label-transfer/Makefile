DATA_DIR=/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/


transform-url-duplicates-to-duplicate-pairs-for-similarity-tests-for-cc15: install
	./src/main/bash/transform-url-duplicates-to-duplicate-pairs-for-similarity-tests-for-cc15.sh

transform-url-duplicates-to-duplicate-pairs-for-similarity-tests: install
	./src/main/bash/transform-url-duplicates-to-duplicate-pairs-for-similarity-tests.sh

url-judgments-from-cw09-to-cw12: install
	hdfs dfs -rm -r -f sigir2021/url-judgments-from-cw09-to-cw12 && \
	./src/main/bash/url-judgments-from-cw09-to-cw12.sh

transform-wayback-warcs-to-collection-documents: install
	hdfs dfs -rm -r -f sigir2021/trec-judgments-in-wayback-machine/redirects-and-snapshots-as-collection-documents.jsonl && \
	./src/main/bash/transform-wayback-warcs-to-collection-documents.sh

url-judgments-from-cw09-or-cw12-to-cc15: install
	./src/main/bash/url-judgments-from-cw09-or-cw12-to-cc15.sh

download-web-2009-redirects: install
	bash -c 'for TOPIC in {10..50}; do ./src/main/bash/download-wayback-redirects.sh -t WEB_2009 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2009/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/redirects/web-2009/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2010-redirects: install
	bash -c 'for TOPIC in {51..100}; do ./src/main/bash/download-wayback-redirects.sh -t WEB_2010 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2010/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/redirects/web-2010/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2011-redirects: install
	bash -c 'for TOPIC in {101..150}; do ./src/main/bash/download-wayback-redirects.sh -t WEB_2011 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2011/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/redirects/web-2011/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2012-redirects: install
	bash -c 'for TOPIC in {151..200}; do ./src/main/bash/download-wayback-redirects.sh -t WEB_2012 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2012/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/redirects/web-2012/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2009-snapshots: install
	bash -c 'for TOPIC in {1..50}; do ./src/main/bash/download-wayback-snapshots.sh -t WEB_2009 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2009/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/snapshots/web-2009/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2010-snapshots: install
	bash -c 'for TOPIC in {51..100}; do ./src/main/bash/download-wayback-snapshots.sh -t WEB_2010 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2010/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/snapshots/web-2010/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2011-snapshots: install
	bash -c 'for TOPIC in {101..150}; do ./src/main/bash/download-wayback-snapshots.sh -t WEB_2011 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2011/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/snapshots/web-2011/topic-$${TOPIC} --topic $${TOPIC} ; done'

download-web-2012-snapshots:
	bash -c 'for TOPIC in {151..200}; do ./src/main/bash/download-wayback-snapshots.sh -t WEB_2012 -i /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2012/topic-$${TOPIC}/ -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/snapshots/web-2012/topic-$${TOPIC} --topic $${TOPIC} ; done'

web-2009: install
	bash -c 'for TOPIC in {1..50}; do ./src/main/bash/trec-judgments-in-wayback-machine.sh -t WEB_2009 --topic $${TOPIC} -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2009/topic-$${TOPIC}/; done'

web-2010:  install
	bash -c 'for TOPIC in {51..100}; do ./src/main/bash/trec-judgments-in-wayback-machine.sh -t WEB_2010 --topic $${TOPIC} -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2010/topic-$${TOPIC}/; done'

web-2011:  install
	bash -c 'for TOPIC in {101..150}; do ./src/main/bash/trec-judgments-in-wayback-machine.sh -t WEB_2011 --topic $${TOPIC} -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2011/topic-$${TOPIC}/; done'

web-2012:  install
	bash -c 'for TOPIC in {151..200}; do ./src/main/bash/trec-judgments-in-wayback-machine.sh -t WEB_2012 --topic $${TOPIC} -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2012/topic-$${TOPIC}/; done'

web-2013:  install
	bash -c 'for TOPIC in {201..250}; do ./src/main/bash/trec-judgments-in-wayback-machine.sh -t WEB_2013 --topic $${TOPIC} -o /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/trec-judgments-in-wayback-machine/web-2012/topic-$${TOPIC}/; done'

jupyter:
	@docker run --rm -ti -p 8888:8888 -v ${PWD}/../research-in-progress/web-search/sigir-21/:/sigir21 -v ${PWD}:/home/jovyan/work -v /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021:/mnt/ceph/storage/data-in-progress/kibi9872/sigir2021 jupyter/datascience-notebook

install:
	./mvnw -T8 clean install 2> /dev/null

simulation-reproducibility-analysis: simulation-test
	@cd simulate-label-transfer/ &&\
	pipenv run python transfer_simulation/evaluation_for_reproduciblity_analysis.py \
		--inputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings \
		--outputFile /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-26-10-2020/rankings/reproducibility-evaluation

simulation-evaluate-original-runs: simulation-test
	@cd simulate-label-transfer/ &&\
	pipenv run python transfer_simulation/original_submitted_runs_evaluation.py \
		--outputFile /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-26-10-2020/rankings/original-runs-evaluation.jsonl

simulation-create-fairness-evaluation: simulation-test
	@cd simulate-label-transfer/ &&\
	pipenv run python transfer_simulation/run_files.py

simulation-create-qrels: simulation-test
	cd simulate-label-transfer/ &&\
	pipenv run python transfer_simulation/label_transfer.py

simulation-test:
	@cd simulate-label-transfer/ &&\
	pipenv run nosetests

simulation-install:
	cd simulate-label-transfer/ &&\
	pipenv install &&\
	pipenv install --dev

ranking-create-original-web-2009: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-1-50-cw09.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-1-50-cw09

ranking-create-original-web-2010: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-51-100-cw09.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-51-100-cw09

ranking-create-original-web-2011: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-101-150-cw09.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-101-150-cw09

ranking-create-original-web-2012: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-151-200-cw09.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-151-200-cw09

ranking-create-original-cw12: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-201-250-cw12.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-201-250-cw12 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-251-300-cw12.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-251-300-cw12

ranking-create-transferred-web-2009: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-1-50-cw12.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-1-50-cw12

ranking-create-transferred-web-2010: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-51-100-cw12.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-51-100-cw12

ranking-create-transferred-web-2011: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-101-150-cw12.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-101-150-cw12

ranking-create-transferred-web-2012: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/ranking-tasks/topics-151-200-cw12.jsonl \
		--outputDir /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/rankings/topics-151-200-cw12

ranking-create-index-clueweb09-in-wayback12: ranking-test
	./ranking/index-wayback-snapshots/index-step01-create-mapfiles.sh &&\
	./ranking/index-wayback-snapshots/index-step02-create-elasticsearch-index.sh &&\
	./ranking/index-wayback-snapshots/index-step03-index-mapfiles-to-elasticsearch.sh &&\
	./ranking/index-wayback-snapshots/index-step04-create-elasticsearch-alias.sh

ranking-create-transferred-cw12-and-wb12: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-1-50-cw12wb12.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-1-50-cw12wb12 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-51-100-cw12wb12.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-51-100-cw12wb12 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-101-150-cw12wb12.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-101-150-cw12wb12 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-151-200-cw12wb12.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-151-200-cw12wb12

ranking-create-transferred-cc15: ranking-test
	@cd ranking/ &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-1-50-cc15.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-1-50-cc15 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-51-100-cc15.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-51-100-cc15 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-101-150-cc15.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-101-150-cc15 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-151-200-cc15.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-151-200-cc15 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-201-250-cc15.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-201-250-cc15 &&\
	pipenv run python main.py \
		--inputJson ${DATA_DIR}ranking-tasks/topics-251-300-cc15.jsonl \
		--outputDir ${DATA_DIR}rankings/topics-251-300-cc15

ranking-test:
	@cd ranking/ &&\
	pipenv run nosetests

ranking-install:
	cd ranking/ &&\
	pipenv install &&\
	pipenv install --dev

plot-similarity: src/main/resources/canonical-edges-s3-aggregations-cc-2015-11.json src/main/resources/canonical-edges-s3-aggregations-cw09.json src/main/resources/canonical-edges-s3-aggregations-cw12.json
	python src/main/python/plot-canonical-similarity.py

plot-transferred: src/main/resources/s3-aggregations-cw-to-cc15.json src/main/resources/s3-aggregations-cw09-to-cw12-and-wayback.json src/main/resources/s3-aggregations-cw09-to-cw12.json
	python src/main/python/plot-canonical-similarity-transferred.py

plots: plot-similarity plot-transferred
