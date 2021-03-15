package de.webis.copycat_spark.spark.eval;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCreateSourceDocumentsIntegrationTest;
import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import de.webis.copycat_spark.spark.eval.SparkAnalyzeCanonicalLinkGraph;

public class SparkAnalyzeCanonicalLinkGraphIntegrationTest extends SparkIntegrationTestBase {
	
	@Test
	public void checkDuplicateGroupCountResultForEmptyInput() {
		List<String> expected = Collections.emptyList();
		List<String> actual = analyze();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkDuplicateGroupCountResultForInputWithoutDuplicates() {
		List<String> expected = Collections.emptyList();
		List<String> actual = analyze(
				entry("http://google.de", "http://google.de"),
				entry("http://google.com", "http://google.com")
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void duplicateGroupCountOnSmallExample() {
		List<String> actual = analyze(
				entry("http://google.de", "http://google.de"),
				entry("http://youtube.de", "http://youtube.de"),
				entry("http://youtube.de", "http://youtube.de"),
				entry("http://youtube.de", "http://youtube.de"),
				entry("http://google.de", "http://google.de")
		);
		
		Approvals.verify(actual);
	}
	

	@Test
	public void checkDomainOverviewResultForEmptyInput() {
		List<String> expected = Collections.emptyList();
		List<String> actual = domainOverview();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkDomainOverviewResultForInputWithoutDuplicates() {
		List<String> expected = Collections.emptyList();
		List<String> actual = domainOverview(
				entry("http://google.de", "http://google.de"),
				entry("http://google.com", "http://google.com")
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void domainOverviewOnSmallExample() {
		List<String> actual = domainOverview(
				entry("http://google.de", "http://google.de"),
				entry("http://youtube.de", "http://youtube.de/abcd"),
				entry("http://youtube.de", "http://youtube.de/abcd"),
				entry("http://example.com", "http://youtube.de/abcd"),
				entry("http://example.com", "http://youtube.de/d"),
				entry("http://youtube.de", "http://youtube.de/d"),
				entry("http://google.de", "http://google.de")
		);
		
		Approvals.verify(actual);
	}

	private List<String> domainOverview(String...elements) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(elements));
		JavaRDD<String> ret = SparkAnalyzeCanonicalLinkGraph.duplicateGroupCountsPerDomain(input);
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private List<String> analyze(String...elements) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(elements));
		JavaRDD<String> ret = SparkAnalyzeCanonicalLinkGraph.duplicateGroupCounts(input);
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String entry(String url, String canonicalLink) {
		return "{\"doc\":{\"url\": \"" + url + "\"}, \"canonicalLink\":\"" + canonicalLink + "\"}";
	}
}
