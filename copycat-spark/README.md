# CopyCat Spark

The spark library of CopyCat detects near-duplicates in large web crawls. The pipeline consists of four steps: (1) calculation of the SimHash fingerprint for each document, (2) selection of one representative document, if multiple documents have identical fingerprints, (3) partition of the remaining fingerprints, (4) calculation of the Hamming distances between all fingerprints of a partition.

# Usage

This repository starts all spark jobs (see the [Makefile](Makefile)) with a spark client from docker.
You only must create your own [conf/hadoop/core-site.xml](conf/hadoop/core-site.xml) file to configure the access to the hadoop/spark cluster.

The following examples show how to run CopyCat on large web crawls:

1. Create Document representations
   - Run `./src/main/bash/new-document-representation-spark.sh` to produce document representations
   - The  [Makefile](Makefile) contains specific examples. E.g., execute `make common-crawl-small-sample-document-representations` to create document representations for a small sample of the common crawl to test your environment.
   - The following commands produce the document-representations used in the paper:
     - `make clueweb09-document-representations`
     - `make clueweb12-document-representations`
     - `make common-crawl15-document-representations`
     - `make common-crawl17-document-representations`

2. Create SimHash Deduplication Jobs
   - This covers the selection of one representative document (if multipledocuments have identical fingerprint), and the partition of the remaining fingerprints into deduplication jobs.
   - After the creation of the document representations, run `./src/main/bash/create-deduplication-candidates.sh` to produce: (1) a list of removed-documents (e.g. too short documents, regarding your configuration); (2) exact-duplicates; and (3) near-duplicate-tasks.
   - The `near-duplicate-tasks` are the blocks that will be fully all-pairs deduplicated in step 3 of the pipeline (Run Deduplication Jobs).
   - The following commands produce the deduplication jobs used in the paper:
     - `make create-deduplication-candidates-cw09`
   
3. Run Deduplication Jobs
   - After the creation of the deduplication jobs, run `./src/main/bash/deduplicate.sh` to produce the pairs of near-duplicates.
   - The following commands produce the document-representations used in the paper:
     - `make deduplicate-cw09`
     - `make deduplicate-cw09-cw12-cc15`

## Development Environment

The Spark library of CopyCat uses maven as build tool.
Please ensure that all maven dependencies are located in your local maven repository by running `make install` in the [root of the repository](..).
To develop in Eclipse, install the [lombok](https://projectlombok.org/) plugin.
Then import this project as "Existing Maven Project" to Eclipse.

