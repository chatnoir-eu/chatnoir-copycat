package de.webis.copycat.anserini;

import org.junit.Assert;
import org.junit.Test;

public class AnseriniIndexDocumentResolverIntegrationTest {
	@Test
	public void testRetrievalOfSingleDocument() {
		String expected = "{\"contents\": \"here is some text here is some more text. city.\"}";
		String actual = docResolver().loadDocumentContent("doc1");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testRetrievalOfDoc2() {
		String expected = "{\"contents\": \"more texts\"}";
		String actual = docResolver().loadDocumentContent("doc2");
		
		Assert.assertEquals(expected, actual);
	}
	

	@Test(expected = Exception.class)
	public void testRetrievalOfNonExistingDocumentFails() {
		docResolver().loadDocumentContent("this-doc-does-not-exist");
	}
	
	private AnseriniIndexDocumentResolver docResolver() {
		return new AnseriniIndexDocumentResolver("src/test/resources/example-anserini-index/");
	}
}
