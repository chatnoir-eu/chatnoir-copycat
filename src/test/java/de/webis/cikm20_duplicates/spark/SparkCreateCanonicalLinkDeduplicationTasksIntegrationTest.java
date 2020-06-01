package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.HashPartitioner;
import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public class SparkCreateCanonicalLinkDeduplicationTasksIntegrationTest extends SharedJavaSparkContext {
	
	@Test
	public void testDeduplicationTasksOfIdenticalDocumentsForDifferentUrls() {
		List<String> expected = Collections.emptyList();
		List<String> actual = deduplicationTasks(
			doc("1", "1", Arrays.asList(1,2,3,4)),
			doc("2", "2", Arrays.asList(1,2,3,4)),
			doc("3", "3", Arrays.asList(1,2,3,4)),
			doc("4", "4", Arrays.asList(1,2,3,4))
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testDeduplicationTasksOfDifferentDocumentsForIdenticalUrls() {
		List<String> expected = Collections.emptyList();
		List<String> actual = deduplicationTasks(
			doc("1", "1", Arrays.asList(1,2,3,4)),
			doc("1", "1", Arrays.asList(5,6,7,8)),
			doc("1", "1", Arrays.asList(9,10,11,12)),
			doc("1", "1", Arrays.asList(13,14,15,16))
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testDeduplicationTasksOfSomeIdenticalDocumentsForSomeIdenticalUrls() {
		List<String> actual = deduplicationTasks(
			doc("1", "1", Arrays.asList(1,2,3,4)),
			doc("2", "2", Arrays.asList(1,2,3,4)),
			doc("3", "3", Arrays.asList(1,2,3,4)),
			doc("4", "4", Arrays.asList(1,2,3,4)),
			doc("5", "1", Arrays.asList(1,2,3,5)),
			doc("6", "1", Arrays.asList(1,2,3,6))
		);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testDeduplicationTasksOfSomeIdenticalDocumentsForSomeIdenticalUrlsAndSomeDuplicates() {
		List<String> actual = deduplicationTasks(
			doc("1", "1", Arrays.asList(1,2,3,4)),
			doc("2", "2", Arrays.asList(1,2,3,4)),
			doc("6", "1", Arrays.asList(1,2,3,6)),
			doc("6", "1", Arrays.asList(1,2,3,6)),
			doc("3", "3", Arrays.asList(1,2,3,4)),
			doc("4", "4", Arrays.asList(1,2,3,4)),
			doc("5", "1", Arrays.asList(1,2,3,5)),
			doc("6", "1", Arrays.asList(1,2,3,6)),
			doc("2", "2", Arrays.asList(1,2,3,4)),
			doc("2", "2", Arrays.asList(1,2,3,4)),
			doc("4", "4", Arrays.asList(1,2,3,4))
		);
		
		Approvals.verifyAsJson(actual);
	}
	
	private List<String> deduplicationTasks(String...docs) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(docs));
		JavaRDD<String> output = SparkCreateCanonicalLinkDeduplicationTasks.urlDeduplicationTask(input, new HashPartitioner(1));
		List<String> ret = new ArrayList<>(output.collect());
		Collections.sort(ret);
		
		return ret;
	}
	
	private static String doc(String id, String canonicalUrl, List<Integer> hashes) {
		return "{\"docId\": \"" + id + "\",\"canonicalURL\":\"https://" + canonicalUrl + ".de\",\"fingerprints\":{\"64BitK3SimHashOneGramms\": " + hashes +"}}";
	}
}
