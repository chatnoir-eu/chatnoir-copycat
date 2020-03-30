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
public class SparkCreateDuplicationCandidatesFromFingerprintsIntegrationTest extends SharedJavaSparkContext {
	
	@Test
	public void testEmptyInput() {
		List<String> actual = duplicationCandidates();
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testInputsWithUnjudgedIds() {
		List<String> expected = Arrays.asList();
		List<String> actual = duplicationCandidates(
				docWithFingerprint("not-in-corpus-1", 1, 1),
				docWithFingerprint("not-in-corpus-2", 1, 1),
				docWithFingerprint("not-in-corpus-3", 1, 1),
				docWithFingerprint("not-in-corpus-4", 1, 1),
				docWithFingerprint("not-in-corpus-5", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputIsReflexive() {
		List<String> expected = Arrays.asList("clueweb09-en0008-02-29970", "clueweb12-1800tw-04-16339");
		List<String> actual = duplicationCandidates(
				docWithFingerprint("not-in-corpus-1", 1, 1),
				docWithFingerprint("clueweb09-en0008-02-29970", 2, 2),
				docWithFingerprint("clueweb12-1800tw-04-16339", 3, 3),
				docWithFingerprint("not-in-corpus-4", 1, 1),
				docWithFingerprint("not-in-corpus-5", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithDuplicatesWithinJudgedDocuments() {
		List<String> expected = Arrays.asList(
				"clueweb09-en0008-02-29970",
				"clueweb12-1800tw-04-16339"
		);
		List<String> actual = duplicationCandidates(
				docWithFingerprint("not-in-corpus-1", 1, 1),
				docWithFingerprint("clueweb09-en0008-02-29970", 2, 2),
				docWithFingerprint("clueweb12-1800tw-04-16339", 2, 2),
				docWithFingerprint("not-in-corpus-4", 1, 1),
				docWithFingerprint("not-in-corpus-5", 1, 1)
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithDuplicatesInJudgedAndUnJudgedDocuments() {
		List<String> expected = Arrays.asList(
				"clueweb09-en0008-02-29970",
				"clueweb12-1800tw-04-16339",
				"not-in-corpus-4"
		);
		List<String> actual = duplicationCandidates(
				docWithFingerprint("not-in-corpus-1", 1, 1),
				docWithFingerprint("clueweb09-en0008-02-29970", 2, 6),
				docWithFingerprint("clueweb12-1800tw-04-16339", 3, 4),
				docWithFingerprint("not-in-corpus-4", 6, 1),
				docWithFingerprint("not-in-corpus-5", 1, 1)
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithAllDuplicates() {
		List<String> expected = Arrays.asList(
				"clueweb09-en0008-02-29970",
				"clueweb12-1800tw-04-16339",
				"not-in-corpus-1",
				"not-in-corpus-4",
				"not-in-corpus-5"
		);
		List<String> actual = duplicationCandidates(
				docWithFingerprint("not-in-corpus-1", 5, 1),
				docWithFingerprint("clueweb09-en0008-02-29970", 1, 2),
				docWithFingerprint("clueweb12-1800tw-04-16339", 3, 4),
				docWithFingerprint("not-in-corpus-4", 2, 7),
				docWithFingerprint("not-in-corpus-5", 3, 9)
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> duplicationCandidates(String...a) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(a));
		JavaRDD<String> ret = SparkCreateDeduplicationCandidates
				.duplicationCandidatesFromFingerprints(input)
				.map(i -> i.getDocId());
		
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String docWithFingerprint(String id, int hash1, int hash2) {
		return "{\"docId\": \"" + id + "\", \"minHashParts\": [" + hash1 + ", " + hash2 + "]}";
	}
}
