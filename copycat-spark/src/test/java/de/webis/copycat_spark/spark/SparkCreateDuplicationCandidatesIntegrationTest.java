package de.webis.copycat_spark.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkCreateDeduplicationCandidates;
import de.webis.copycat_spark.spark.SparkCreateSourceDocumentsIntegrationTest.DummyAnseriniCollectionReader;
import de.webis.copycat_spark.util.FingerPrintUtil;
import de.webis.copycat_spark.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateDuplicationCandidatesIntegrationTest extends SparkIntegrationTestBase {

	private Fingerprinter<Integer> fingerprinter;
	
	@Before
	public void setUp() {
		fingerprinter = FingerPrintUtil.minHashFingerPrinting(1);
	}
	
	@Test
	public void testWithEmptySourceAndEmptyAcr() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList());
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader();
		
		List<String> actual = transfrom(input, acr);
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testWithSomeSourceAndEmptyAcr() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
				"{\"doc\":{\"id\":\"id-1\",\"content\":\"content of id-1\",\"fullyCanonicalizedContent\":\"a b c d e f g h i j k\"},\"topics\":[\"CLUEWEB12::SESSION_2013::52\", \"CLUEWEB12::SESSION_2013::53\", \"CLUEWEB12::SESSION_2013::54\"]}",
				"{\"doc\":{\"id\":\"id-2\",\"content\":\"content of id-2\",\"fullyCanonicalizedContent\":\"a a a a a a a a a a a\"},\"topics\":[\"CLUEWEB12::SESSION_2013::53\"]}"
		));
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader();
		
		List<String> actual = transfrom(input, acr);
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testWithEmptySourceAndSomeAcr() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList());
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader(
				doc("a b c d e f g h i j k"),
				doc("a a a a a a a a a a a"),
				doc("a a a a a a a a a a b"),
				doc("b a a a a a a a a a b"),
				doc("b a b a a a a c a a b"),
				doc("b a a f a a d a a a b")
				
		);
		
		List<String> actual = transfrom(input, acr);
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testWithSomeSourceAndSomeAcr() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
				"{\"doc\":{\"id\":\"id-1\",\"content\":\"content of id-1\",\"fullyCanonicalizedContent\":\"a b c d e f g h i j k\"},\"topics\":[\"CLUEWEB12::SESSION_2013::52\", \"CLUEWEB12::SESSION_2013::53\", \"CLUEWEB12::SESSION_2013::54\"]}",
				"{\"doc\":{\"id\":\"id-2\",\"content\":\"content of id-2\",\"fullyCanonicalizedContent\":\"a a a a a a a a a a a\"},\"topics\":[\"CLUEWEB12::SESSION_2013::53\"]}"
		));
		AnseriniCollectionReader<Document> acr = new DummyAnseriniCollectionReader(
				doc("a b c d e f g h i j k"),
				doc("a a a a a a a a a a a"),
				doc("a a a a a a a a a a b"),
				doc("b a a a a a a a a a b"),
				doc("b a b a a a a c a a b"),
				doc("a b c d e f g h i j i"),
				doc("b a a f a a d a a a b")
				
		);
		
		List<String> actual = transfrom(input, acr);
		Approvals.verifyAsJson(actual);
	}
	
	private List<String> transfrom(JavaRDD<String> sourceDocuments, AnseriniCollectionReader<Document> acr) {
		JavaRDD<Tuple2<String, CollectionDocument>> ret = SparkCreateDeduplicationCandidates.candidatesForAllSourceDocuments(jsc(), sourceDocuments, fingerprinter, acr);
		return sorted(ret);
	}

	private static CollectionDocument doc(String fullyCanonicalizedContent) {
		return new CollectionDocument("id-" + fullyCanonicalizedContent, "content of " + fullyCanonicalizedContent, fullyCanonicalizedContent, null, null, null);
	}
	
	public static List<String> sorted(JavaRDD<?> rdd) {
		List<String> ret = rdd.map(i -> i.toString()).collect();
		ret = new ArrayList<>(ret);
		Collections.sort(ret);
		
		return ret;
	}
}
