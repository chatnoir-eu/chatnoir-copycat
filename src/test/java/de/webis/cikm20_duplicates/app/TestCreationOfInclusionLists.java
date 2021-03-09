package de.webis.cikm20_duplicates.app;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

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
	
	private List<String> inclusionList(List<String> allIds, List<String> excludedIds) {
		return CreationOfInclusionLists.createInclusionList(
			jsc().parallelize(allIds), 
			jsc().parallelize(excludedIds),
			1
		).collect();
	}
}
