package de.webis.cikm20_duplicates.util;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.util.SourceDocuments.SourceDocument;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SourceDocumentsTest {
	@Test
	public void approveSourceDocumentsOfAllTasks() {
		List<SourceDocument> actual = SourceDocuments.ALL_DOCS_FOR_WHICH_DUPLICATES_SHOULD_BE_SEARCHED;
		
		Approvals.verifyAsJson(actual.stream().map(i -> i.toString()).collect(Collectors.toList()));
	}
	
	@Test
	public void approveCountOfDocsToTransfer() {
		int expected = 102789;
		int actual = SourceDocuments.ALL_DOCS_FOR_WHICH_DUPLICATES_SHOULD_BE_SEARCHED.size();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveTaskCount() {
		int expected = 6;
		int actual = SourceDocuments.taskToDocuments().keySet().size();

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveTopicCount() {
		int expected = 298;
		int actual = SourceDocuments.topicsToDocumentIds().keySet().size();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDocCountPerTopic() {
		Map<String, Set<String>> actual = SourceDocuments.topicsToDocumentIds();
		Map<String, Integer> toApprove = new LinkedHashMap<>(); 
		actual.entrySet().forEach(i -> toApprove.put(i.getKey(), i.getValue().size()));
		
		Approvals.verifyAsJson(toApprove);
	}
	
	@Test
	public void approveDocCountPerTask() {
		Map<String, Integer> toApprove = new LinkedHashMap<>(); 
		SourceDocuments.taskToDocuments().entrySet().forEach(i -> toApprove.put(i.getKey().name(), i.getValue().size()));
		

		Approvals.verifyAsJson(toApprove);
	}
}
