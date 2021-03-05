# CopyCat

CopyCat is an open-source resource to make deduplication in TREC-style experimental setups more attainable and provides (1) a compilation of near-duplicate documents within the ClueWeb09, the ClueWeb12, and two Common Crawl snapshots, and (2) a software library enabling the deduplication of arbitrary document sets.

## Outline

- [Getting Started](#getting-started)
- [Inclusion and Exclusion lists of near-duplicates for the ClueWebs and two Common Crawl snapshots](https://webis.de/data/chatnoir-copycat-21)
- [Deduplication of Run and Qrel files](#deduplication-of-run-and-qrel-files)
- [Deduplication of Large Webcrawls with the CopyCat Spark Pipeline](deduplication-of-large-crawls-with-spark)
- [Documentation on Document-Preprocessing](copycat-modules/document-preprocessing/README.md) (Used in all parts of the software)
- [Showcases](#showcases)
- [Tutorial: ClueWeb09 (Category B) Index without Near-Duplicates](case-studies/indexing-clueweb09b-without-near-duplicates/README.md)
- [Development Environment](#development-environment)

## Getting Started

CopyCat provides an docker image with support for jupyter notebooks (+ [common data science libraries](https://hub.docker.com/r/jupyter/datascience-notebook/)).

To start a bash shell with CopyCat installed and your local directory mounted, run 
```
docker run --rm -ti -v ${PWD}:/home/jovyan webis/chatnoir-copycat:1.0-jupyter bash 
```
In this bash, you have CopyCat installed. E.g., to show the help, run `copy-cat --help`, which prints:
```
usage: CopyCat: Deduplication of run files and qrels.
       [-h] --input INPUT --output OUTPUT
       [--similarities {url,s3,cosine(3+5-grams),cosine(8-grams),cosine(1-grams),simhash(1-grams),simhash(3+5-grams),md5,text-profile} [{url,s3,cosine(3+5-grams),cosine(8-grams),cosine(1-grams),simhash(1-grams),simhash(3+5-grams),md5,text-profile} ...]]
       --documents {ChatNoirMapfiles,AnseriniIndex} [--anseriniIndex ANSERINIINDEX] [--retrieveDocId RETRIEVEDOCID] [--ranks RANKS]
       [--s3Threshold S3THRESHOLD] [--threads THREADS] [--runFile {true,false}] [--keepStopwords {true,false}]
       [--contentExtraction {Anserini,Boilerpipe,Jericho,No}] [--stemmer {porter,krovetz,null}] [--stopwords STOPWORDS]

named arguments:
  -h, --help             show this help message and exit
  --input INPUT          The run file or qrel file that should be deduplicated.
  --output OUTPUT        The result of the deduplication in jsonl format.
  --similarities {url,s3,cosine(3+5-grams),cosine(8-grams),cosine(1-grams),simhash(1-grams),simhash(3+5-grams),md5,text-profile} [{url,s3,cosine(3+5-grams),cosine(8-grams),cosine(1-grams),simhash(1-grams),simhash(3+5-grams),md5,text-profile} ...]
                         Calculate all passed similarities.
  --documents {ChatNoirMapfiles,AnseriniIndex}
                         Use the passed DocumentResolver to load  the  documents.  E.g.  AnseriniIndex  loads  documents by accessing a local
                         anserini-index.
  --anseriniIndex ANSERINIINDEX
                         When using AnseriniIndex as resolver for documents, we use the specified index.
  --retrieveDocId RETRIEVEDOCID
                         Retrieve a single document from and print it to  the  console.  This  is  useful to check the preprocessing on a few
                         example documents.
  --ranks RANKS          Include documents up to the specified rank in the deduplication.
  --s3Threshold S3THRESHOLD
                         Report only near-duplicate pairs with s3 scores on word 8-grams above the specified threshold.
  --threads THREADS
  --runFile {true,false}
                         Is the specified a run file (pass true), or a qrels file (pass false)
  --keepStopwords {true,false}
                         Switch: keep stopwords or remove them.
  --contentExtraction {Anserini,Boilerpipe,Jericho,No}
                         The name of the content extraction. (Use  'Anserini'  for  Anserini's  default HTML to plain text transformation, or
                         'No' in case documents are already transformed (e.g., because they come from an anserini index)
  --stemmer {porter,krovetz,null}
                         The name of the stemmer (passed to Lucene with Anserini).
  --stopwords STOPWORDS  The list of stopwords is read from this  file.  When  keepStopwords  is  false, and stopwords = null, then Anserinis
                         default is used.
```


To start a jupyter notebook with CopyCat installed and your local directory mounted, run:
```
docker run --rm -ti -v ${PWD}:/home/jovyan -p 8888:8888 webis/chatnoir-copycat:1.0-jupyter
```
Now you can point your browser to [localhost:8888](localhost:8888) to access the notebook.


## Deduplication of Run and Qrel Files

CopyCat provides an docker image to support various deduplication experiments on standard IR run and qrel files for various test collections.
The following List provides some examples on how to use the docker image to deduplicate run/qrel files:

- [Robust04](src/main/jupyter/copycat-on-robust04.ipynb)
- [Touche 2020](src/main/jupyter/copycat-on-argsme.ipynb)

## Showcases

This repository contains the two showcases from the paper.
As a general introduction, see the [Getting Started](#getting-started) section.

- Deduplication of run/qrel files with the copycat-cli ([copycat-on-cluweb.ipynb](src/main/jupyter/copycat-on-cluweb-runs.ipynb))
- [Transfer of relevance labels](case-studies/relevance-label-transfer/README.md)

## Development Environment:

Please install:

- Java 8 (our hadoop cluster runs hadoop 2.8, hence we need to compile to be compatible with java 8)
- Maven
- [Project Lombok](https://projectlombok.org/) to your IDE (used to remove a bit of boilerplate code)
- [https://approvaltests.com/](https://approvaltests.com/) (especially the diff-tools, this is used in unit-tests)
- Docker


## Deduplication of Large Crawls with Spark

To detect near-duplicates in large web crawls, copycat runs a pipeline of four steps: (1) calculation of the SimHash fingerprint for each document, (2) selection of one representative document, if multipledocuments have identical fingerprints, (3) partition of the remaining fingerprints, (4) calculation of the Hamming distances between allfingerprints of a partition.

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

