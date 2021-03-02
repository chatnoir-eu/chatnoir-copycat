# Showcase for the CopyCat Library: "Transferring Relevance Judgments"

This directory contains all the code and required resources and enables reproducing the experiments from the paper and running the algorithms on new corpora. To simplify the documentation of our steps, all steps are persisted in a [Makefile](Makefile).

Outline

- [Transferred Relevance Judgments](#transferred-relevance-judgments): The resulting new relevance judgments on ClueWeb12, ClueWeb12+, and Common Crawl 2015.
- [ClueWeb12+](#clueweb12): Steps to produce our simulated ClueWeb12+. ClueWeb12+ is the ClueWeb12 plus snapshots of judged web pages not included in the ClueWeb12 crawl, but available in the ClueWeb12 crawling period in the Wayback Machine.
- [Evaluation](#evaluation): All evaluations reported in the paper.
- [(Re)Producing Transferred Relevance Judgments](#reproducing-transferred-relevance-judgments): The steps used to identify near-duplicates between judged documents in the old corpora and documents in the new corpora.


## Transferred Relevance Judgments

We have [produced new relevance judgments](#reproducing-transferred-relevance-judgments) by finding (near-)duplicates of judged ClueWeb09/ClueWeb12 documents in newer crawls.
All transferred relevance judgments are located in [src/main/resources/artificial-qrels/](src/main/resources/artificial-qrels/).

Here is an overview of the judgments:

- Original judgments (we removed near-duplicates judged for the same topic):
  - [Web Track 2009](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.web.1-50.txt)
  - [Web Track 2010](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.web.51-100.txt)
  - [Web Track 2011](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.web.101-150.txt)
  - [Web Track 2012](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.web.151-200.txt)
  - [Web Track 2013](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.web.201-250.txt)
  - [Web Track 2014](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.web.251-300.txt)
- Transferred judgments to ClueWeb12:
  - [Web Track 2009](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.1-50.txt)
  - [Web Track 2010](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.51-100.txt)
  - [Web Track 2011](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.101-150.txt)
  - [Web Track 2012](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cw12.151-200.txt)
- Transferred judgments to Common Crawl 2015:
  - [Web Track 2009](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.1-50.txt)
  - [Web Track 2010](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.51-100.txt)
  - [Web Track 2011](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.101-150.txt)
  - [Web Track 2012](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.151-200.txt)
  - [Web Track 2013](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.201-250.txt)
  - [Web Track 2014](src/main/resources/artificial-qrels/qrels.inofficial.duplicate-free.transferred-to-cc15.251-300.txt)

The associated topics can be found [in Anserini](https://github.com/castorini/anserini/tree/master/src/main/resources/topics-and-qrels).

## ClueWeb12+

The creation of ClueWeb12+ was performed in two steps (please install `java:8`, then run `make install` to install all dependencies):

1. We use the Wayback CDX-Api to find the timestamps when judged URLs are saved in the Wayback Machine: [de.webis.sigir2021.App](src/main/java/de/webis/sigir2021/App.java)
2. We crawl the snapshots identified by the CDX-Api from the Wayback Machine: [de.webis.sigir2021.CrawlWaybackSnapshots](src/main/java/de/webis/sigir2021/CrawlWaybackSnapshots.java)

Both steps create WARC files.
The resulting WARC files are available online, combined in a single tar here:
  - [https://files.webis.de/sigir2021-relevance-label-transfer-clueweb12-plus/clueweb12-plus-warcs-from-wayback-machine.tar.xz](https://files.webis.de/sigir2021-relevance-label-transfer-clueweb12-plus/clueweb12-plus-warcs-from-wayback-machine.tar.xz)

The results of both steps are also available as the intermediate topic-level WARCs, which can be found here:
  - [https://files.webis.de/sigir2021-relevance-label-transfer-clueweb12-plus/](https://files.webis.de/sigir2021-relevance-label-transfer-clueweb12-plus/).
  
We have indexed the ClueWeb12+ in Elasticsearch with `make ranking-create-index-clueweb09-in-wayback12`.

## Evaluation

Please install `docker`, `python:3.6`, and `pipenv`.
There are [make targets](Makefile) for all our experiment steps.

### Preparations

- Install `docker`, `python:3.6`, and `pipenv`
- Install dependencies by running: `make simulation-install` and `make ranking-install`
- Verify your installation by running: `make simulation-test` and `make ranking-test` (you can skip ranking-test when you want to reuse our run-files)

### Produce Evaluations Reported in the Paper

All jupyter notebooks with some additional experiments can be found in [src/main/jupyter](src/main/jupyter), and we can start jupyter with `make jupyter`.

### Produce Evaluations on Original Run-Files

- Download the run-files from [TREC](https://trec.nist.gov/results/).
- Run `make simulation-evaluate-original-runs` to produce [original-runs-evaluation.jsonl](src/main/resources/original-runs-evaluation.jsonl)

### Produce Rankings from the Case-Study

We set up the elasticsearch indices with `make ranking-create-index-clueweb09-in-wayback12`.
Use the associated make targets to create the run-files (the run files are also available online at [https://files.webis.de/sigir2021-relevance-label-transfer-resources/case-study-run-files/](https://files.webis.de/sigir2021-relevance-label-transfer-resources/case-study-run-files/)):

- `make ranking-create-original-web-<TRACK_YEAR>` to create the original run-files for `<TRACK_YEAR>`.
- `make ranking-create-transferred-web-<TRACK_YEAR>` to create the transferred run-files for `<TRACK_YEAR>`.
- `make ranking-create-transferred-cw12-and-wb12` to create all transferred run files for ClueWeb12+.
- `make ranking-create-transferred-cc15` to create all transferred run files for Common Crawl 2015.
- When all run-files are available, run `make simulation-reproducibility-analysis` to produce [reproducibility-evaluation-per-query-zero-scores-removed.jsonl](src/main/resources/reproducibility-evaluation-per-query-zero-scores-removed.jsonl) (this jsonl file is used to create the evaluations in section "5.2 Experiments with Best-Case Topic Selections")

## (Re)Producing Transferred Relevance Judgments

We create the transferred relevance judgments in two steps:

- First, we create a jsonl file for each track that contains near-duplicates for the judgments with the class [de.webis.sigir2021.trec.LabelTransfer](src/main/java/de/webis/sigir2021/trec/LabelTransfer.java)
- Second, we use those jsonl files to create qrel files for each track with the class [de.webis.sigir2021.trec.CreateQrels](src/main/java/de/webis/sigir2021/trec/CreateQrels.java)

All resources for creating the transferred relevance judgments are available online at [https://files.webis.de/sigir2021-relevance-label-transfer-resources/](https://files.webis.de/sigir2021-relevance-label-transfer-resources/):

- [https://files.webis.de/sigir2021-relevance-label-transfer-resources/url-transfer-from-cw09-to-cw12-with-similarity.jsonl](https://files.webis.de/sigir2021-relevance-label-transfer-resources/url-transfer-from-cw09-to-cw12-with-similarity.jsonl)
- [https://files.webis.de/sigir2021-relevance-label-transfer-resources/relevance-transfer-only-near-duplicates.jsonl](https://files.webis.de/sigir2021-relevance-label-transfer-resources/relevance-transfer-only-near-duplicates.jsonl)
- [https://files.webis.de/sigir2021-relevance-label-transfer-resources/relevance-transfer-exact-duplicates.jsonl](https://files.webis.de/sigir2021-relevance-label-transfer-resources/relevance-transfer-exact-duplicates.jsonl)
- [https://files.webis.de/sigir2021-relevance-label-transfer-resources/url-transfer-from-cw09-or-cw12-to-cc15-with-similarity.jsonl](https://files.webis.de/sigir2021-relevance-label-transfer-resources/url-transfer-from-cw09-or-cw12-to-cc15-with-similarity.jsonl)

This resources are created with (please install `java:8`, `hadoop:2.7.1`, `spark:2.2.1`):

- [near-duplicate detection with SimHash](simhash-near-duplicate-candidates)
- Transfer to Near-Duplicates with identical/canonical URLs with the class [de.webis.sigir2021.Evaluation](src/main/java/de/webis/sigir2021/Evaluation.java) and [de.webis.sigir2021.EvaluationCC15](src/main/java/de/webis/sigir2021/EvaluationCC15.java)
  - All similarities are prepared with [spark](src/main/java/de/webis/sigir2021/spark) and calculated [here](simhash-near-duplicate-candidates/src/main/java/de/webis/cikm20_duplicates/app/EnrichPairsOfDocumentsWithS3SCore.java), but the results of this calculation can be downloaded 
    - [https://files.webis.de/sigir2021-relevance-label-transfer-resources/wayback-similarities.jsonl](https://files.webis.de/sigir2021-relevance-label-transfer-resources/wayback-similarities.jsonl)
    - [https://files.webis.de/sigir2021-relevance-label-transfer-resources/url-judgments-from-cw09-to-cw12.jsonl](https://files.webis.de/sigir2021-relevance-label-transfer-resources/url-judgments-from-cw09-to-cw12.jsonl)
  - Please download the ClueWeb12+ resources: 
    - [https://files.webis.de/sigir2021-relevance-label-transfer-clueweb12-plus/clueweb12-plus-warcs-from-wayback-machine.tar.xz](https://files.webis.de/sigir2021-relevance-label-transfer-clueweb12-plus/clueweb12-plus-warcs-from-wayback-machine.tar.xz)

