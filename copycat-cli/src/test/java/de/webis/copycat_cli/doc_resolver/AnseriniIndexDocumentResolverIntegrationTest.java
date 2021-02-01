package de.webis.copycat_cli.doc_resolver;

import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat.DocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class AnseriniIndexDocumentResolverIntegrationTest {
	@Test
	public void testRetrievalOfSingleDocument() {
		String expected = "{\"contents\": \"here is some text here is some more text. city.\"}";
		String actual = docResolver().loadCollectionDocument("doc1").getContent();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testRetrievalOfDoc2() {
		String expected = "{\"contents\": \"more texts\"}";
		CollectionDocument actual = docResolver().loadCollectionDocument("doc2");
		
		Assert.assertEquals(expected, actual.getContent());
		Assert.assertEquals("doc2", actual.getId());
	}
	
	@Test(expected = Exception.class)
	public void testRetrievalOfNonExistingDocumentFails() {
		docResolver().loadCollectionDocument("this-doc-does-not-exist");
	}
	
	private DocumentResolver docResolver() {
		return new AnseriniDocumentResolver("src/test/resources/example-anserini-index/");
	}
}
