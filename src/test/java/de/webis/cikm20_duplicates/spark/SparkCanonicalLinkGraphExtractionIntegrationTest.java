package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.io.Text;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import scala.Tuple2;

public class SparkCanonicalLinkGraphExtractionIntegrationTest extends SharedJavaSparkContext {

	@Test
	public void checkEmptyDocuments() {
		List<String> expected = Collections.emptyList();
		List<String> actual = canonicalLinkEdges(rdd());
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void approveDocumentWithSingleCanonicalLink() {
		String expected = "[{\"doc\":{\"id\":\"21\",\"content\":\"Hello World\",\"fullyCanonicalizedContent\":\"hello world\",\"url\":\"http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353\"},\"canonicalLink\":\"http://example.com/\",\"crawlingTimestamp\":\"2015-03-05T14:33:50Z\"}]";
		List<String> actual = canonicalLinkEdges(rdd(html("21", "<html><head><link rel=\"canonical\" href=\"http://example.com/\"></head><body>Hello World</body></html>")));

		Assert.assertEquals(expected, actual.toString());
	}
	
	@Test
	public void approveDocumentWithSingleCanonicalLinkWithoutId() {
		String expected = "[{\"doc\":{\"id\":\"-1172634332\",\"content\":\"Hello World\",\"fullyCanonicalizedContent\":\"hello world\",\"url\":\"http://www.necrohiphop.com/forum/ubbthreads.php?ubb=showflat&Number=244311&page=3353\"},\"canonicalLink\":\"http://example.com/\",\"crawlingTimestamp\":\"2015-03-05T14:33:50Z\"}]";
		List<String> actual = canonicalLinkEdges(rdd(htmlWithoutTrecId("<html><head><link rel=\"canonical\" href=\"http://example.com/\"></head><body>Hello World</body></html>")));

		Assert.assertEquals(expected, actual.toString());
	}
	
	@Test
	public void approveDocumentWithCanonicalLinkAtWrongPosition() {
		List<String> expected = Collections.emptyList();
		List<String> actual = canonicalLinkEdges(rdd(html("21", "<html><body><link rel=\"canonical\" href=\"http://example.com/\"></body></html>")));

		Assert.assertEquals(expected, actual);
	}
	
	private List<String> canonicalLinkEdges(JavaPairRDD<Text, Text> input) {
		JavaRDD<String> ret = SparkCanonicalLinkGraphExtraction.canonicalLinkedges(input);
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
