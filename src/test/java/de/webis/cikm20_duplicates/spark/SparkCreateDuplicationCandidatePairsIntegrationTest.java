package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateDuplicationCandidatePairsIntegrationTest extends SharedJavaSparkContext {

	@Test
	public void testEmptyInput() {
		List<String> expected = Arrays.asList();
		List<String> actual = duplicationPairs();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithAllPairsUnsimilar() {
		List<String> expected = Arrays.asList();
		List<String> actual = duplicationPairs(
				docWithFingerprint("not-in-corpus-1", 1, 2),
				docWithFingerprint("not-in-corpus-2", 3, 4),
				docWithFingerprint("not-in-corpus-3", 5, 6),
				docWithFingerprint("not-in-corpus-4", 7, 8),
				docWithFingerprint("not-in-corpus-5", 9, 10)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithAllPairsSimilar() {
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-2\"}",
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-3\"}",
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-4\"}",
			"{\"firstId\":\"not-in-corpus-2\",\"secondId\":\"not-in-corpus-3\"}",
			"{\"firstId\":\"not-in-corpus-2\",\"secondId\":\"not-in-corpus-4\"}",
			"{\"firstId\":\"not-in-corpus-3\",\"secondId\":\"not-in-corpus-4\"}"
		);
		List<String> actual = duplicationPairs(
				docWithFingerprint("not-in-corpus-1", 1, 1),
				docWithFingerprint("not-in-corpus-2", 1, 1),
				docWithFingerprint("not-in-corpus-3", 1, 1),
				docWithFingerprint("not-in-corpus-4", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSomeSimilarPairs() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"a\"}",
				"{\"firstId\":\"0\",\"secondId\":\"b\"}",
				"{\"firstId\":\"a\",\"secondId\":\"b\"}"
			);
		
		List<String> actual = duplicationPairs(
				docWithFingerprint("a", 1, 1),
				docWithFingerprint("clueweb09-en0008-02-29970", 2, 2),
				docWithFingerprint("clueweb12-1800tw-04-16339", 3, 3),
				docWithFingerprint("0", 1, 1),
				docWithFingerprint("b", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSingleSimilarPair() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"clueweb09-en0008-02-29970\"}"
			);
		
		List<String> actual = duplicationPairs(
				docWithFingerprint("a", 1, 1),
				docWithFingerprint("clueweb09-en0008-02-29970", 2, 2),
				docWithFingerprint("clueweb12-1800tw-04-16339", 3, 3),
				docWithFingerprint("0", 2, 2),
				docWithFingerprint("b", 4, 4)
		);

		Assert.assertEquals(expected, actual);
	}
	
	private List<String> duplicationPairs(String...a) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(a));
		JavaRDD<?> ret = SparkCreateDeduplicationCandidates.duplicationCandidatePairsFromFingerprints(input);
		
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String docWithFingerprint(String id, int hash1, int hash2) {
		return "{\"docId\": \"" + id + "\", \"minHashParts\": [" + hash1 + ", " + hash2 + "]}";
	}
}
