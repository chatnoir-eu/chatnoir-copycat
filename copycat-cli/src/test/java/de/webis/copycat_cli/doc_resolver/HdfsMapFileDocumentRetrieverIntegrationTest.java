package de.webis.copycat_cli.doc_resolver;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat.DocumentResolver;
import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class HdfsMapFileDocumentRetrieverIntegrationTest {
	@Test
	public void approveSampleCw09Document() {
		CollectionDocument doc = docResolver().loadCollectionDocument("clueweb09-en0002-17-16080");
		
		Approvals.verifyAsJson(doc);
	}
	
	@Test
	public void approveSampleSecondCw09Document() {
		CollectionDocument doc = docResolver().loadCollectionDocument("clueweb09-en0009-61-13707");
		
		Approvals.verifyAsJson(doc);
	}
	
	@Test
	public void approveDocumentWithEncodingProblems() {
		CollectionDocument doc = docResolver().loadCollectionDocument("clueweb12-1610wb-71-00602");
		
		Assert.assertNull(doc);
	}
	
	public DocumentResolver docResolver() {
		return new ChatNoirDocumentResolver();
	}
}
