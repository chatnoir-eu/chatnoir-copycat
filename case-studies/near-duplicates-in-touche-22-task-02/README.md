# Detect Near-Duplicates in the Collection of Touché 2022 Task 2

### Step 1: Transform passages to the input format of CopyCat

```
java -cp copycat-cli de.webis.copycat_cli.doc_resolver.IrDatasetsDocumentResolver --input /mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/2020-task2-passages-of-top100-docs.jsonl --output /mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/2022-task2/data-cleaning/2020-task2-passages-of-top100-docs.jsonl
java -cp copycat-cli de.webis.copycat_cli.doc_resolver.IrDatasetsDocumentResolver
--input /mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/2021-task2-passages-of-top100-docs.jsonl --output /mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/2022-task2/data-cleaning/2021-task2-passages-of-top100-docs.jsonl

```

### Step 2: Run CopyCat

```
./run-s3-deduplication.sh 2020-task2-passages-of-top100-docs
./run-s3-deduplication.sh 2021-task2-passages-of-top100-docs
```

### Step 3: Create document

```
docker run \
	--rm -ti -p 8888:8888 \
	-v "${PWD}":/home/jovyan/work \
	-v /mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/:/mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/ \
	jupyter/datascience-notebook
```
