# Create an Anserini Index for ClueWeb12 (Category B13) without Near-Duplicates

This tutorial shows how to use the inclusion list from CopyCat for the ClueWeb12b13 with [Anserini](https://github.com/castorini/anserini/blob/master/docs/regressions-cw12b13.md).
Documents on the inclusion list are never near-duplicates of each other.

## Step 1: Install Anserini

```
git clone https://github.com/castorini/anserini.git
cd anserini
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
mvn clean package appassembler:assemble
```

## Step 2: Indexing

Please download the inclusion list of CopyCat for ClueWeb12 Category B13:
```
curl 'https://corpus-copycat.s3.data.webis.de/near-duplicate-free-inclusion-lists/cw12b13/part-00000.bz2' |bzip2 -d > cw12b13-inclusion-list
```

Create the Anserini-Index with Anserini (where `/path/to/ClueWeb12b13` is the root directory of the ClueWeb12 (Category B13), i.e. `ls /path/to/ClueWeb12b13` returns `ClueWeb12_00` to `ClueWeb12_18`):
```
target/appassembler/bin/IndexCollection -collection ClueWeb12Collection \
    -input /path/to/ClueWeb12b13 \
    -whitelist cw12b13-inclusion-list \
    -index indexes/lucene-index.cw12b13.pos+docvectors+raw \
    -generator DefaultLuceneDocumentGenerator \
    -threads 44 -storePositions -storeDocvectors -storeRaw \
    >& logs/log.cw12b13
```

