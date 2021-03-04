# Preprocessing of Documents for CopyCat

This module contains all document preprocessing strategies implemented in CopyCat.
All preprocessing strategies are covered by [Approval tests](https://approvaltests.com/) which also provide good examples to see [how documents are preprocessed for the different configrations](src/test/java/de/webis/copycat/document_preprocessing/).

- All preprocessing options from [Anserini](https://github.com/castorini/anserini/blob/master/src/main/java/io/anserini/index/IndexArgs.java) are supported.
  - It supports:
    - `--keepStopwords`
    - `--stopwords`: The list of stopwords is read from this file. When keepStopwords is false, and stopwords = null, then Anserinis default is used.
    - `stemmer`:  The name of the stemmer (passed to Lucene with Anserini). Choices: "porter", "krovetz", "null"
    - `contentExtraction`: The name of the content extraction. (Use 'Anserini' for Anserini's default HTML to plain text transformation, or 'No' in case documents are already transformed (e.g., because they come from an anserini index). Choices: "Anserini", "Boilerpipe", "Jericho", "No"
- Main content extraction using:
  - Boilerpipe
  - Chatnoirs main extraction based on Jericho

- 
