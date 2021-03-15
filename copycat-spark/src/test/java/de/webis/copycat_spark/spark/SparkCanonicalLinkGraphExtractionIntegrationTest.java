package de.webis.copycat_spark.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCanonicalLinkGraphExtraction;
import scala.Tuple2;

public class SparkCanonicalLinkGraphExtractionIntegrationTest extends SparkIntegrationTestBase {

	@Test
	public void checkEmptyDocuments() {
		List<String> expected = Collections.emptyList();
		List<String> actual = canonicalLinkEdges(rdd(), null);
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void approveDocumentWithSingleCanonicalLink() {
		String expected = "[{\"doc\":{\"id\":\"21\",\"content\":\"Hello World\",\"fullyCanonicalizedContent\":\"hello world\",\"crawlingTimestamp\":null,\"url\":\"http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353\",\"canonicalUrl\":null},\"canonicalLink\":\"http://example.com/\",\"crawlingTimestamp\":\"2015-03-05T14:33:50Z\"}]";
		List<String> actual = canonicalLinkEdges(rdd(html("21", "<html><head><link rel=\"canonical\" href=\"http://example.com/\"></head><body>Hello World</body></html>")), null);

		Assert.assertEquals(expected, actual.toString());
	}
	
	@Test
	public void approveDocumentWithSingleCanonicalLinkWithoutId() {
		String expected = "[{\"doc\":{\"id\":\"-1172634332\",\"content\":\"Hello World\",\"fullyCanonicalizedContent\":\"hello world\",\"crawlingTimestamp\":null,\"url\":\"http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353\",\"canonicalUrl\":null},\"canonicalLink\":\"http://example.com/\",\"crawlingTimestamp\":\"2015-03-05T14:33:50Z\"}]";
		List<String> actual = canonicalLinkEdges(rdd(htmlWithoutTrecId("<html><head><link rel=\"canonical\" href=\"http://example.com/\"></head><body>Hello World</body></html>")), null);

		Assert.assertEquals(expected, actual.toString());
	}
	
	@Test
	public void approveDocumentWithSingleCanonicalLinkWithoutIdButWithEmptyUrlsToKeep() {
		List<String> expected = Collections.emptyList();
		List<String> actual = canonicalLinkEdges(rdd(htmlWithoutTrecId("<html><head><link rel=\"canonical\" href=\"http://example.com/\"></head><body>Hello World</body></html>")), Collections.emptySet());

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveDocumentWithSingleCanonicalLinkWithoutIdButWithSingleUrlToKeep() {
		Set<String> urlsToKeep = new HashSet<>();
		urlsToKeep.add("http://example.com/");
		
		String expected = "[{\"doc\":{\"id\":\"-1172634332\",\"content\":\"Hello World\",\"fullyCanonicalizedContent\":\"hello world\",\"crawlingTimestamp\":null,\"url\":\"http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353\",\"canonicalUrl\":null},\"canonicalLink\":\"http://example.com/\",\"crawlingTimestamp\":\"2015-03-05T14:33:50Z\"}]";
		List<String> actual = canonicalLinkEdges(rdd(htmlWithoutTrecId("<html><head><link rel=\"canonical\" href=\"http://example.com/\"></head><body>Hello World</body></html>")), urlsToKeep);

		Assert.assertEquals(expected, actual.toString());
	}
	
	@Test
	public void approveDocumentWithCanonicalLinkAtWrongPosition() {
		List<String> expected = Collections.emptyList();
		List<String> actual = canonicalLinkEdges(rdd(html("21", "<html><body><link rel=\"canonical\" href=\"http://example.com/\"></body></html>")), null);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void das() {
		Set<String> expected = new HashSet<>(Arrays.asList(
			"http://www.homedepot.com/p/Croydex-48-in-Shower-Cubicle-Telescopic-Rod-in-White-AD100022YW/203549766",
			"http://www.toysrus.com/buy/animation/back-at-the-barnyard-when-no-one-s-looking-dvd-1358501-3250473",
			"http://www.mrlock.com/american-lock-padlock-2010-solid-steel-a2010"
			
		));
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"url\":\"http://www.mrlock.com/american-lock-padlock-2010-solid-steel-a2010\",\"count\":2}",
			"{\"url\":\"http://www.homedepot.com/p/Croydex-48-in-Shower-Cubicle-Telescopic-Rod-in-White-AD100022YW/203549766\",\"count\":2}",
			"{\"url\":\"http://www.toysrus.com/buy/animation/back-at-the-barnyard-when-no-one-s-looking-dvd-1358501-3250473\",\"count\":4}"
		));
		Set<String> actual = SparkCanonicalLinkGraphExtraction.canonicalLinksToKeep(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> canonicalLinkEdges(JavaPairRDD<Text, Text> input, Set<String> urlsToKeep) {
		JavaRDD<String> ret = SparkCanonicalLinkGraphExtraction.canonicalLinkedges(input, urlsToKeep);
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private JavaPairRDD<Text, Text> rdd(String...jsons) {
		return jsc().parallelize(Arrays.asList(jsons))
				.mapToPair(i -> new Tuple2<Text, Text>(new Text(i.hashCode()+""), new Text(i)));
	}
	
	private String htmlWithoutTrecId(String html) {
		return "{'metadata': {'WARC-Target-URI': 'http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353',\n" + 
				"    'WARC-Date': '2015-03-05T14:33:50Z',\n" +
				"    'WARC-Type': 'response'}," + 
				"   'payload': {'body': '" + html + "'," + 
				"    'encoding': 'plain'}}";
	}
	
	private String html(String id, String html) {
		return "{'metadata': {'WARC-Target-URI': 'http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353',\n" + 
				"    'WARC-Date': '2015-03-05T14:33:50Z',\n" + 
				"    'WARC-TREC-ID': '" + id + "',\n" + 
				"    'WARC-Type': 'response'}," + 
				"   'payload': {'body': '" + html + "'," + 
				"    'encoding': 'plain'}}";
	}
}
