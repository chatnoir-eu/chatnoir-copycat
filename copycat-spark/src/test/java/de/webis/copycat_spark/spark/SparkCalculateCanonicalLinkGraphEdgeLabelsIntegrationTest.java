package de.webis.copycat_spark.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCalculateCanonicalLinkGraphEdgeLabels;

public class SparkCalculateCanonicalLinkGraphEdgeLabelsIntegrationTest extends SparkIntegrationTestBase {

	@Test
	public void checkEdgeLabelsResultForEmptyInput() {
		List<String> expected = Collections.emptyList();
		List<String> actual = edgeLabels();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkEdgeLabelsForInputWithoutDuplicates() {
		List<String> expected = Collections.emptyList();
		List<String> actual = edgeLabels(
				entry("http://google.de", "http://google.de"),
				entry("http://google.com", "http://google.com")
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void edgeLabelsOnSmallExample() {
		List<String> expected = Arrays.asList(
			"{\"canonicalLink\":\"http://google.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://google.de/1\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://google.de/1\",\"canonicalUrl\":null},\"canonicalLink\":\"http://google.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://google.de/5\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://google.de/5\",\"canonicalUrl\":null},\"canonicalLink\":\"http://google.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}",
			"{\"canonicalLink\":\"http://youtube.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://youtube.de/2\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://youtube.de/2\",\"canonicalUrl\":null},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://youtube.de/3\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://youtube.de/3\",\"canonicalUrl\":null},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}",
			"{\"canonicalLink\":\"http://youtube.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://youtube.de/2\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://youtube.de/2\",\"canonicalUrl\":null},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://youtube.de/4\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://youtube.de/4\",\"canonicalUrl\":null},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}",
			"{\"canonicalLink\":\"http://youtube.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://youtube.de/3\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://youtube.de/3\",\"canonicalUrl\":null},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://youtube.de/4\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"crawlingTimestamp\":null,\"url\":\"http://youtube.de/4\",\"canonicalUrl\":null},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}"
			
		);
		List<String> actual = edgeLabels(
				entry("http://google.de/1", "http://google.de"),
				entry("http://youtube.de/2", "http://youtube.de"),
				entry("http://youtube.de/3", "http://youtube.de"),
				entry("http://youtube.de/4", "http://youtube.de"),
				entry("http://google.de/5", "http://google.de")
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void edgeLabelsOnSmallExampleWithInvalidCanonicalLinks() {
		List<String> expected = Collections.emptyList();
		List<String> actual = edgeLabels(
				entry("http://google.de/1", "https://google.de{}"),
				entry("http://youtube.de/2", "https://youtube.de{}"),
				entry("http://youtube.de/3", "https://youtube.de{}"),
				entry("http://youtube.de/4", "https://youtube.de{}"),
				entry("http://google.de/5", "https://google.de{}")
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkThatWeObtain100RandomStrings() {
		Set<String> seen = new HashSet<>();
		Supplier<String> suppl = SparkCalculateCanonicalLinkGraphEdgeLabels::randomStringForGroupSplitting;
		
		for(int i=0; i< 10000; i++) {
			seen.add(suppl.get());
		}
		
		Assert.assertEquals(100, seen.size());
	}
	
	private List<String> edgeLabels(String...elements) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(elements));
		JavaRDD<String> ret = SparkCalculateCanonicalLinkGraphEdgeLabels.edgeLabels(input, new HashPartitioner(2));
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String entry(String url, String canonicalLink) {
		return "{\"doc\":{\"url\": \"" + url + "\",\"id\":\"" + url + "\",\"content\":\"\", \"fullyCanonicalizedContent\":\"\"}, \"canonicalLink\":\"" + canonicalLink + "\"}";
	}
}
