# Create an Anserini Index for ClueWeb09 (Category B) without Near-Duplicates

This tutorial shows how to use the inclusion list from CopyCat for the ClueWeb09b with [Anserini](https://github.com/castorini/anserini/blob/master/docs/regressions-cw09b.md).
Documents on the inclusion list are never near-duplicates of each other.

## Step 1: Install Anserini

```
git clone https://github.com/castorini/anserini.git
cd anserini
export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64/
mvn clean package appassembler:assemble
```

## Step 2: Indexing

Please download the inclusion list of CopyCat for ClueWeb09 Category B:
```
curl 'https://corpus-copycat.s3.data.webis.de/near-duplicate-free-inclusion-lists/cw09b/part-00000.bz2' |bzip2 -d > cw09b-inclusion-list
```

Create the Anserini-Index with Anserini (where `/path/to/ClueWeb09b` is the root directory of the ClueWeb09 (Category B), i.e. ls /path/to/ClueWeb09b returns en0000 to enwp03, du -hs /path/to/ClueWeb09b should list ~230GB):
```
target/appassembler/bin/IndexCollection -collection ClueWeb09Collection \
    -input /path/to/cw09b \
    -whitelist cw09b-inclusion-list \
    -index indexes/lucene-index.cw09b.pos+docvectors+raw \
    -generator DefaultLuceneDocumentGenerator \
    -threads 44 -storePositions -storeDocvectors -storeRaw \
    >& logs/log.cw09b
```

