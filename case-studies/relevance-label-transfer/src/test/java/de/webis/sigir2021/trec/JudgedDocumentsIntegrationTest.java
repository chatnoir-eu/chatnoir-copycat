package de.webis.sigir2021.trec;

import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.trec.JudgedDocuments;
import de.webis.trec_ndd.trec_collections.SharedTask;
import de.webis.trec_ndd.trec_collections.SharedTask.TrecSharedTask;

public class JudgedDocumentsIntegrationTest {
	@Test
	public void approveUrlOfFirstJudgedWeb2009Document() {
		String expected = "http://www.cellphonenews.com/category/usa_canada_phones/3g_cell_phones";
		SharedTask task = TrecSharedTask.WEB_2009;
		String docId = task.documentJudgments().getRelevantDocuments("34").get(0);
		
		String actual = JudgedDocuments.urlOfDocument(docId);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveUrlOfNonExistingDoc() {
		String actual = JudgedDocuments.urlOfDocument("clueweb09-en0000-44-22367");
		
		Assert.assertNull(actual);
	}
	
	@Test
	public void approveUrlOfFirstJudgedWeb2011Document() {
		String expected = "http://www.tottevents.com/index.php/Virtual-Reality/Laser-Skeet-Shooting";
		SharedTask task = TrecSharedTask.WEB_2011;
		String docId = task.documentJudgments().getRelevantDocuments("134").get(0);
		
		String actual = JudgedDocuments.urlOfDocument(docId);
		
		Assert.assertEquals(expected, actual);
	}
}
