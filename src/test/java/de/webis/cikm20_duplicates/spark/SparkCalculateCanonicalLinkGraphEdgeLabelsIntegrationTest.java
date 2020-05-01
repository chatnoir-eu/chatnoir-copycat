package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public class SparkCalculateCanonicalLinkGraphEdgeLabelsIntegrationTest extends SharedJavaSparkContext {

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
			"{\"canonicalLink\":\"http://google.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://google.de/1\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://google.de/1\"},\"canonicalLink\":\"http://google.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://google.de/5\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://google.de/5\"},\"canonicalLink\":\"http://google.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}",
			"{\"canonicalLink\":\"http://youtube.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://youtube.de/2\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://youtube.de/2\"},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://youtube.de/3\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://youtube.de/3\"},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}",
			"{\"canonicalLink\":\"http://youtube.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://youtube.de/2\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://youtube.de/2\"},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://youtube.de/4\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://youtube.de/4\"},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}",
			"{\"canonicalLink\":\"http://youtube.de\",\"firstDoc\":{\"doc\":{\"id\":\"http://youtube.de/3\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://youtube.de/3\"},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"secondDoc\":{\"doc\":{\"id\":\"http://youtube.de/4\",\"content\":\"\",\"fullyCanonicalizedContent\":\"\",\"url\":\"http://youtube.de/4\"},\"canonicalLink\":\"http://youtube.de\",\"crawlingTimestamp\":null},\"s3score\":1.0}"
			
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
	
	private List<String> edgeLabels(String...elements) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(elements));
		JavaRDD<String> ret = SparkCalculateCanonicalLinkGraphEdgeLabels.edgeLabels(input, new HashPartitioner(2));
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String entry(String url, String canonicalLink) {
		return "{\"doc\":{\"url\": \"" + url + "\",\"id\":\"" + url + "\",\"content\":\"\", \"fullyCanonicalizedContent\":\"\"}, \"canonicalLink\":\"" + canonicalLink + "\"}";
	}
}