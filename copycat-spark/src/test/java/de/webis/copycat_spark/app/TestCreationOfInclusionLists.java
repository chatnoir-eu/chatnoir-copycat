package de.webis.copycat_spark.app;

import java.util.Arrays;
import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import net.sourceforge.argparse4j.inf.Namespace;

public class TestCreationOfInclusionLists extends SparkIntegrationTestBase {

	@Test
	public void testEverythingIsIncludedWhenExclusionListIsEmpty() {
		List<String> allIds = Arrays.asList("a", "b", "c");
		List<String> excludedIds = Arrays.asList();
		List<String> expected = Arrays.asList("a", "b", "c");
		List<String> actual = inclusionList(allIds, excludedIds, false);
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testIdsOnExclusionListAreNotInOutput() {
		List<String> allIds = Arrays.asList("a", "b", "c");
		List<String> excludedIds = Arrays.asList("b");
		List<String> expected = Arrays.asList("a", "c");
		List<String> actual = inclusionList(allIds, excludedIds, false);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testIdsOnExclusionListAreNotInOutputForDocRepresentations() {
		List<String> allIds = Arrays.asList(
			"{\"docId\":\"a\",\"url\":\"http://foo-bar.com/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[],\"64BitK3SimHashThreeAndFiveGramms\":[]}}",
			"{\"docId\":\"b\",\"url\":\"http://foo-bar.com \",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[],\"64BitK3SimHashThreeAndFiveGramms\":[]}}",
			"{\"docId\":\"c\",\"url\":\"http://foo-bar.com\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[],\"64BitK3SimHashThreeAndFiveGramms\":[]}}"
		);
		List<String> excludedIds = Arrays.asList("b");
		List<String> expected = Arrays.asList("a", "c");
		List<String> actual = inclusionList(allIds, excludedIds, true);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testEverythingIsIncludedWhenExclusionListIsEmptyForDocRepresentations() {
		List<String> allIds = Arrays.asList(
			"{\"docId\":\"a\",\"url\":\"http://foo-bar.com/\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[],\"64BitK3SimHashThreeAndFiveGramms\":[]}}",
			"{\"docId\":\"b\",\"url\":\"http://foo-bar.com \",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[],\"64BitK3SimHashThreeAndFiveGramms\":[]}}",
			"{\"docId\":\"c\",\"url\":\"http://foo-bar.com\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[],\"64BitK3SimHashThreeAndFiveGramms\":[]}}"
		);
		List<String> excludedIds = Arrays.asList();
		List<String> expected = Arrays.asList("a", "b", "c");
		List<String> actual = inclusionList(allIds, excludedIds, true);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> inclusionList(List<String> allIds, List<String> excludedIds, boolean useDocumentRepresentations) {
		Namespace args = CreationOfInclusionLists.validArgumentsOrNull(new String[]{
			"--allIds", "a",
			"--exclusionIds", "b",
			"-o", "a",
			"--docRepresentations", String.valueOf(useDocumentRepresentations),
			"--partitions", "1"
		});
		
		return CreationOfInclusionLists.createInclusionList(
			jsc().parallelize(allIds), 
			jsc().parallelize(excludedIds),
			args
		).collect();
	}
}
