# CopyCat: Near-Duplicate Detection for Web Crawls

## Pipeline

ToDo: Easy to use examples: I only have git + docker, and then I can run the following near-duplicate detection on corpora on my local machine (inspired by capreolus).

1. Create Document representations
   - Run `./src/main/bash/new-document-representation-spark.sh` to produce document representations
     ```
     TODO: Example + the example is covered by a make target
     ```
   - The  [Makefile](Makefile) contains specific examples. E.g., execute `make common-crawl-small-sample-document-representations` to create document representations for a small sample of the common crawl to test your environment.
   - The following commands produce the document-representations used in the paper:
     - `make clueweb09-document-representations`
     - `make clueweb12-document-representations`
     - `make common-crawl15-document-representations`
     - `make common-crawl17-document-representations`

2. Create SimHash Deduplication Jobs
   - After the creation of the document representations, run `./src/main/bash/create-deduplication-candidates.sh` to produce: (1) a list of removed-documents (e.g. too short documents, regarding your configuration); (2) exact-duplicates; and (3) near-duplicate-tasks.
   - The `near-duplicate-tasks` are the blocks that will be fully all-pairs deduplicated in step 3 of the pipeline (Run Deduplication Jobs).
   - The following commands produce the deduplication jobs used in the paper:
     - `make create-deduplication-candidates-cw09`
   
3. Run Deduplication Jobs
   - After the creation of the deduplication jobs, run `./src/main/bash/deduplicate.sh` to produce the pairs of near-duplicates.
   - The following commands produce the document-representations used in the paper:
     - `make deduplicate-cw09`

