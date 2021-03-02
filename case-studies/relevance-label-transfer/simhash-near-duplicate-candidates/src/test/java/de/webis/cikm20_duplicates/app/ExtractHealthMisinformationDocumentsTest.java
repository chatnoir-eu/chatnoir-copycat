package de.webis.cikm20_duplicates.app;

import java.util.ArrayList;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.trec_ndd.trec_collections.CollectionDocument;

public class ExtractHealthMisinformationDocumentsTest {
	@Test
	public void checkTransformationOfIDs1() {
		CollectionDocument expected = new CollectionDocument();
		expected.setId("49ecaf74-b1aa-4563-83a0-c81cece0e284");
		expected.setContent("aaaaa");
		
		CollectionDocument doc = new CollectionDocument();
		doc.setId("<urn:uuid:49ecaf74-b1aa-4563-83a0-c81cece0e284>");
		doc.setContent("aaaaa");
		
		CollectionDocument actual = ExtractHealthMisinformationDocuments.fixId(doc);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkTransformationOfIDs2() {
		CollectionDocument expected = new CollectionDocument();
		expected.setId("a123");
		expected.setContent("bbb");
		
		CollectionDocument doc = new CollectionDocument();
		doc.setId("<urn:uuid:a123>");
		doc.setContent("bbb");
		
		CollectionDocument actual = ExtractHealthMisinformationDocuments.fixId(doc);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveIdsToKeep() {
		List<String> actual = new ArrayList<>(ExtractHealthMisinformationDocuments.idsToKeep());
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveCcNewsFiles() {
		List<String> ret = ExtractHealthMisinformationDocuments.ccNewsLinks();
		
		Approvals.verifyAsJson(ret);
	}
}
