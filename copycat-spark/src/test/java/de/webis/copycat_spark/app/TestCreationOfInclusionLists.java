package de.webis.copycat_spark.app;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkIntegrationTestBase;

public class TestCreationOfInclusionLists extends SparkIntegrationTestBase {

	@Test
	public void testEverythingIsIncludedWhenExclusionListIsEmpty() {
		List<String> allIds = Arrays.asList("a", "b", "c");
		List<String> excludedIds = Arrays.asList();
		List<String> expected = Arrays.asList("a", "b", "c");
		List<String> actual = inclusionList(allIds, excludedIds);
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testIdsOnExclusionListAreNotInOutput() {
		List<String> allIds = Arrays.asList("a", "b", "c");
		List<String> excludedIds = Arrays.asList("b");
		List<String> expected = Arrays.asList("a", "c");
		List<String> actual = inclusionList(allIds, excludedIds);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testIdsOnExclusionListAreNotInOutputForDocRepresentations() {
		List<String> allIds = Arrays.asList(
			"{\"docId\":\"a\",\"url\":\"http://www.porn2.com/videosz/alexis-malone-cyanara-fox-share-a-cock-and-a-kiss/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[471793664,56756,15007767,2217472],\"64BitK3SimHashThreeAndFiveGramms\":[-1012924416,3860,6488085,8476416]}}",
			"{\"docId\":\"b\",\"url\":\"http://www.porn2.com/videosz/alexis-malone-cyanara-fox-share-a-cock-and-a-kiss/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[471793664,56756,15007767,2217472],\"64BitK3SimHashThreeAndFiveGramms\":[-1012924416,3860,6488085,8476416]}}",
			"{\"docId\":\"c\",\"url\":\"http://www.porn2.com/videosz/alexis-malone-cyanara-fox-share-a-cock-and-a-kiss/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[471793664,56756,15007767,2217472],\"64BitK3SimHashThreeAndFiveGramms\":[-1012924416,3860,6488085,8476416]}}"
		);
		List<String> excludedIds = Arrays.asList("b");
		List<String> expected = Arrays.asList("a", "c");
		List<String> actual = inclusionListForDocumentRepresentations(allIds, excludedIds);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testEverythingIsIncludedWhenExclusionListIsEmptyForDocRepresentations() {
		List<String> allIds = Arrays.asList(
			"{\"docId\":\"a\",\"url\":\"http://www.porn2.com/videosz/alexis-malone-cyanara-fox-share-a-cock-and-a-kiss/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[471793664,56756,15007767,2217472],\"64BitK3SimHashThreeAndFiveGramms\":[-1012924416,3860,6488085,8476416]}}",
			"{\"docId\":\"b\",\"url\":\"http://www.porn2.com/videosz/alexis-malone-cyanara-fox-share-a-cock-and-a-kiss/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[471793664,56756,15007767,2217472],\"64BitK3SimHashThreeAndFiveGramms\":[-1012924416,3860,6488085,8476416]}}",
			"{\"docId\":\"c\",\"url\":\"http://www.porn2.com/videosz/alexis-malone-cyanara-fox-share-a-cock-and-a-kiss/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[471793664,56756,15007767,2217472],\"64BitK3SimHashThreeAndFiveGramms\":[-1012924416,3860,6488085,8476416]}}"
		);
		List<String> excludedIds = Arrays.asList();
		List<String> expected = Arrays.asList("a", "b", "c");
		List<String> actual = inclusionListForDocumentRepresentations(allIds, excludedIds);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> inclusionListForDocumentRepresentations(List<String> allIds, List<String> excludedIds) {
		return CreationOfInclusionLists.createInclusionListForDocumentRepresentations(
			jsc().parallelize(allIds), 
			jsc().parallelize(excludedIds),
			1
		).collect();
	}
	
	private List<String> inclusionList(List<String> allIds, List<String> excludedIds) {
		return CreationOfInclusionLists.createInclusionList(
			jsc().parallelize(allIds), 
			jsc().parallelize(excludedIds),
			1
		).collect();
	}
}
