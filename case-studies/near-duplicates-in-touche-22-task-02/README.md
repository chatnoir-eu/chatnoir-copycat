# Detect Near-Duplicates in the Collection of Touché 2022 Task 2

### Step 1: Transform passages to the input format of CopyCat

```
DATA_DIR=/mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/ ./transform-data-to-copycat-input.py --input ${DATA_DIR}2020-task2-passages-of-top100-docs.jsonl --output ${DATA_DIR}2022-task2/data-cleaning/2020-task2-passages-of-top100-docs.jsonl
DATA_DIR=/mnt/ceph/storage/data-in-progress/data-research/arguana/touche-shared-tasks/data/ ./transform-data-to-copycat-input.py --input ${DATA_DIR}2021-task2-passages-of-top100-docs.jsonl --output ${DATA_DIR}2022-task2/data-cleaning/2021-task2-passages-of-top100-docs.jsonl
```

### Step 2: Run CopyCat

```
./run-s3-deduplication.sh 2020-task2-passages-of-top100-docs
./run-s3-deduplication.sh 2021-task2-passages-of-top100-docs
```

