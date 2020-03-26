package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.shaded.com.google.common.collect.Iterators;
import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.ImmutableList;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkGroupSourceDocumentsByTopicIntegrationTest extends SharedJavaSparkContext {
	
	private Fingerprinter<Integer> fingerprinter;
	
	@Before
	public void setUp() {
		fingerprinter = FingerPrintUtil.minHashFingerPrinting(1);
	}
	
	@Test
	public void testWithEmptyDocuments() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList());
		
		Map<String, List<CollectionDocument>> actual = transfrom(input);
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void approveWithDisjointTopics() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
				"{\"doc\":{\"id\":\"id-1\",\"content\":\"content of id-1\",\"fullyCanonicalizedContent\":\"a b c d e f g h i j k\"},\"topics\":[\"CLUEWEB12::SESSION_2013::52\"]}",
				"{\"doc\":{\"id\":\"id-2\",\"content\":\"content of id-2\",\"fullyCanonicalizedContent\":\"a a a a a a a a a a a\"},\"topics\":[\"CLUEWEB12::SESSION_2013::53\"]}"
		));
		
		Map<String, List<CollectionDocument>> actual = transfrom(input);
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveWithOverlappingTopics() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
				"{\"doc\":{\"id\":\"id-1\",\"content\":\"content of id-1\",\"fullyCanonicalizedContent\":\"a b c d e f g h i j k\"},\"topics\":[\"CLUEWEB12::SESSION_2013::52\", \"CLUEWEB12::SESSION_2013::53\", \"CLUEWEB12::SESSION_2013::54\"]}",
				"{\"doc\":{\"id\":\"id-2\",\"content\":\"content of id-2\",\"fullyCanonicalizedContent\":\"a a a a a a a a a a a\"},\"topics\":[\"CLUEWEB12::SESSION_2013::53\"]}"
		));
		
		Map<String, List<CollectionDocument>> actual = transfrom(input);
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void approveMinHashShinglesPerTopicWithOverlappingTopics() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
				"{\"doc\":{\"id\":\"id-1\",\"content\":\"content of id-1\",\"fullyCanonicalizedContent\":\"a b c d e f g h i j k\"},\"topics\":[\"CLUEWEB12::SESSION_2013::52\", \"CLUEWEB12::SESSION_2013::53\", \"CLUEWEB12::SESSION_2013::54\"]}",
				"{\"doc\":{\"id\":\"id-2\",\"content\":\"content of id-2\",\"fullyCanonicalizedContent\":\"a a a a a a a a a a a\"},\"topics\":[\"CLUEWEB12::SESSION_2013::53\"]}"
		));
		
		List<Tuple2<String, Set<Integer>>> tmp = SparkCreateDeduplicationCandidates.topicsToFingerPrintsOfImportantDocsPerTopic(input, fingerprinter).collect();
		tmp = new ArrayList<>(tmp);
		Collections.sort(tmp, (a,b) -> a.toString().compareTo(b.toString()));
		
		Map<String, Set<Integer>> actual = new LinkedHashMap<>();
		for(Tuple2<String, Set<Integer>> p: tmp) {
			actual.put(p._1(), p._2());
		}
		
		Approvals.verifyAsJson(actual);
	}
	
	private Map<String, List<CollectionDocument>> transfrom(JavaRDD<String> rdd) {
		List<Tuple2<String, Iterable<Tuple2<String, CollectionDocument>>>> ret = SparkCreateDeduplicationCandidates.topicsToImportantDocuments(rdd).collect();
		ret = new ArrayList<>(ret);
		Collections.sort(ret, (a,b) -> a.toString().compareTo(b.toString()));
		
		Map<String, List<CollectionDocument>> ret2 = new LinkedHashMap<>();
		for(Tuple2<String, Iterable<Tuple2<String, CollectionDocument>>> tmp : ret) {
			ret2.put(tmp._1, ImmutableList.copyOf(Iterators.transform(tmp._2.iterator(), i -> i._2)));
		}
		
		return ret2;
	}
}
