package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationStrategy;

public class SparkExtractExactDuplicatesIntegrationTest extends SharedJavaSparkContext {

	@Test
	public void testWithEmptyInput() {
		List<String> expected = Collections.emptyList();
		List<String> actual = duplicationCandidates(DeduplicationStrategy.minHashDeduplication(5));
		
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testWithInputWithoutDuplicates() {
		List<String> expected = Collections.emptyList();
		List<String> actual = duplicationCandidates(
				DeduplicationStrategy.minHashDeduplication(5),
				minHashDoc("id-1", 1, 1),
				minHashDoc("id-2", 2, 2),
				minHashDoc("id-3", 3, 3),
				minHashDoc("id-4", 4, 4),
				minHashDoc("id-5", 5, 5)
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithInputWithOneDuplicateGroup() {
		List<String> actual = duplicationCandidates(
				DeduplicationStrategy.minHashDeduplication(5),
				minHashDoc("id-1", 1, 1),
				minHashDoc("id-2", 1, 1),
				minHashDoc("id-3", 1, 1),
				minHashDoc("id-4", 1, 1),
				minHashDoc("id-5", 1, 1)
		);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testWithInputWithOneDuplicateGroup2() {
		List<String> actual = duplicationCandidates(
				DeduplicationStrategy.minHashDeduplication(5),
				minHashDoc("id-3", 1, 1),
				minHashDoc("id-4", 1, 1),
				minHashDoc("id-1", 1, 1),
				minHashDoc("id-2", 1, 1),
				minHashDoc("id-5", 1, 1)
		);
		
		Approvals.verifyAsJson(actual);
	}
	
	
	@Test
	public void testWithInputWithMultipleGroups() {
		List<String> actual = duplicationCandidates(
				DeduplicationStrategy.minHashDeduplication(5),
				minHashDoc("id-3", 1, 1),
				minHashDoc("id-4", 3, 3),
				minHashDoc("id-1", 1, 1),
				minHashDoc("id-2", 2, 1),
				minHashDoc("id-5", 2, 1)
		);
		
		Approvals.verifyAsJson(actual);
	}
	
	private List<String> duplicationCandidates(DeduplicationStrategy dedupStrategy, String...a) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(a));
		JavaRDD<String> ret = SparkCreateDeduplicationCandidates
				.exactDuplicates(input, dedupStrategy);
		
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String minHashDoc(String id, Integer...hashes) {
		return docWithFingerprint(id, Arrays.asList(hashes), Arrays.asList(9999, 9999));
	}
	
	private static String docWithFingerprint(String id, List<Integer> minHashParts, List<Integer> simHash65BitParts) {
		return "{\"docId\": \"" + id + "\",\"fingerprints\":{\"MinHashWithJavaHash\": " + minHashParts + ", \"64BitK3SimHashOneGramms\": " + simHash65BitParts + "}}";
	}
}
