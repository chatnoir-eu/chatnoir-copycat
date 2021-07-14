package de.webis.copycat_cli.doc_resolver;

import java.util.List;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class AnseriniIndexToCollectionDocumentIntegrationTest {
	@Test
	public void testTransformationOfAllDocuments() {
		AnseriniDocumentResolver docResolver = docResolver();
		List<CollectionDocument> docs = docResolver.allDocumentsInIndex().collect(Collectors.toList());
		Approvals.verifyAsJson(docs);
	}
	
	private AnseriniDocumentResolver docResolver() {
		return new AnseriniDocumentResolver("src/test/resources/example-anserini-index/");
	}
}
