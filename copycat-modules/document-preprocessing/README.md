# Preprocessing of Documents for CopyCat

This module contains all document preprocessing strategies implemented in CopyCat.
All preprocessing strategies are covered by [Approval tests](https://approvaltests.com/) which also provide good examples to see [how documents are preprocessed for the different configrations](src/test/java/de/webis/copycat/document_preprocessing/).

- All preprocessing options from [Anserini](https://github.com/castorini/anserini/blob/master/src/main/java/io/anserini/index/IndexArgs.java) are supported.
- Main content extraction using:
  - Boilerpipe
  - Chatnoirs main extraction based on Jericho

- 
