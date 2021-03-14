package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkCreateDeduplicationCandidates.DeduplicationStrategy;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateDuplicationCandidatePairsIntegrationTest extends SparkIntegrationTestBase {

	@Test
	public void testEmptyInputForMinHash() {
		List<String> expected = Arrays.asList();
		List<String> actual = minHashDuplicationPairs();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testEmptyInputForSimHash() {
		List<String> expected = Arrays.asList();
		List<String> actual = simHashDuplicationPairs();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithAllPairsUnsimilarForMinHash() {
		List<String> expected = Arrays.asList();
		List<String> actual = minHashDuplicationPairs(
				minHashesDoc("not-in-corpus-1", 1, 2),
				minHashesDoc("not-in-corpus-2", 3, 4),
				minHashesDoc("not-in-corpus-3", 5, 6),
				minHashesDoc("not-in-corpus-4", 7, 8),
				minHashesDoc("not-in-corpus-5", 9, 10)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithAllPairsUnsimilarForSimHash() {
		List<String> expected = Arrays.asList();
		List<String> actual = simHashDuplicationPairs(
				simHashesDoc("not-in-corpus-1", 1, 2),
				simHashesDoc("not-in-corpus-2", 3, 4),
				simHashesDoc("not-in-corpus-3", 5, 6),
				simHashesDoc("not-in-corpus-4", 7, 8),
				simHashesDoc("not-in-corpus-5", 9, 10)
		);

		Assert.assertEquals(expected, actual);
	}
	
	//,"firstFingerprintComponents":[1,1],"secondFingerprintComponents":[1,1]
	
	@Test
	public void testInputsWithAllPairsSimilarForMinHash() {
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-2\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-3\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-4\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-2\",\"secondId\":\"not-in-corpus-3\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-2\",\"secondId\":\"not-in-corpus-4\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-3\",\"secondId\":\"not-in-corpus-4\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}"
		);
		List<String> actual = minHashDuplicationPairs(
				minHashesDoc("not-in-corpus-1", 1, 1),
				minHashesDoc("not-in-corpus-2", 1, 1),
				minHashesDoc("not-in-corpus-3", 1, 1),
				minHashesDoc("not-in-corpus-4", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testInputsWithAllPairsSimilarForSimHash() {
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-2\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-3\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-1\",\"secondId\":\"not-in-corpus-4\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-2\",\"secondId\":\"not-in-corpus-3\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-2\",\"secondId\":\"not-in-corpus-4\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
			"{\"firstId\":\"not-in-corpus-3\",\"secondId\":\"not-in-corpus-4\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}"
		);
		List<String> actual = simHashDuplicationPairs(
				simHashesDoc("not-in-corpus-1", 1, 1),
				simHashesDoc("not-in-corpus-2", 1, 1),
				simHashesDoc("not-in-corpus-3", 1, 1),
				simHashesDoc("not-in-corpus-4", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSomeSimilarPairsForMinHash() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"a\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
				"{\"firstId\":\"0\",\"secondId\":\"b\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
				"{\"firstId\":\"a\",\"secondId\":\"b\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}"
			);
		
		List<String> actual = minHashDuplicationPairs(
				minHashesDoc("a", 1, 1),
				minHashesDoc("clueweb09-en0008-02-29970", 2, 2),
				minHashesDoc("clueweb12-1800tw-04-16339", 3, 3),
				minHashesDoc("0", 1, 1),
				minHashesDoc("b", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSomeSimilarPairsForSimHash() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"a\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
				"{\"firstId\":\"0\",\"secondId\":\"b\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}",
				"{\"firstId\":\"a\",\"secondId\":\"b\",\"firstFingerprintComponents\":[1,1],\"secondFingerprintComponents\":[1,1]}"
			);
		
		List<String> actual = simHashDuplicationPairs(
				simHashesDoc("a", 1, 1),
				simHashesDoc("clueweb09-en0008-02-29970", 2, 2),
				simHashesDoc("clueweb12-1800tw-04-16339", 3, 3),
				simHashesDoc("0", 1, 1),
				simHashesDoc("b", 1, 1)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSingleSimilarPairForMinHash() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"clueweb09-en0008-02-29970\",\"firstFingerprintComponents\":[2,2],\"secondFingerprintComponents\":[2,2]}"
			);
		
		List<String> actual = minHashDuplicationPairs(
				minHashesDoc("a", 1, 1),
				minHashesDoc("clueweb09-en0008-02-29970", 2, 2),
				minHashesDoc("clueweb12-1800tw-04-16339", 3, 3),
				minHashesDoc("0", 2, 2),
				minHashesDoc("b", 4, 4)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSingleSimilarPairForSimHash() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"clueweb09-en0008-02-29970\",\"firstFingerprintComponents\":[2,2],\"secondFingerprintComponents\":[2,2]}"
			);
		
		List<String> actual = simHashDuplicationPairs(
				simHashesDoc("a", 1, 1),
				simHashesDoc("clueweb09-en0008-02-29970", 2, 2),
				simHashesDoc("clueweb12-1800tw-04-16339", 3, 3),
				simHashesDoc("0", 2, 2),
				simHashesDoc("b", 4, 4)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSingleSimilarPairForMinHashWithLongElementList() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"clueweb09-en0008-02-29970\",\"firstFingerprintComponents\":[-10,-5,2,-1],\"secondFingerprintComponents\":[10,12,2,5]}"
			);
		
		List<String> actual = minHashDuplicationPairs(
				minHashesDoc("a", 1, 1),
				minHashesDoc("clueweb09-en0008-02-29970", 10, 12, 2, 5),
				minHashesDoc("clueweb12-1800tw-04-16339", 3, 3),
				minHashesDoc("0", -10, -5, 2, -1),
				minHashesDoc("b", 4, 4)
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSingleSimilarPairForSimHashWithLongElementList() {
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"0\",\"secondId\":\"clueweb09-en0008-02-29970\",\"firstFingerprintComponents\":[-10,-5,2,-1],\"secondFingerprintComponents\":[10,11,2,5]}"
			);
		
		List<String> actual = simHashDuplicationPairs(
				simHashesDoc("a", 1, 1),
				simHashesDoc("clueweb09-en0008-02-29970", 10, 11, 2, 5),
				simHashesDoc("clueweb12-1800tw-04-16339", 3, 3),
				simHashesDoc("0", -10, -5, 2, -1),
				simHashesDoc("b", 4, 4)
		);

		Assert.assertEquals(expected, actual);
	}
	
	private List<String> simHashDuplicationPairs(String...a) {
		return duplicationPairs(DeduplicationStrategy.simHashDeduplication(10), a);
	}
	
	private List<String> minHashDuplicationPairs(String...a) {
		return duplicationPairs(DeduplicationStrategy.minHashDeduplication(10), a);
	}
	
	
	private List<String> duplicationPairs(DeduplicationStrategy dedup, String...a) {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(a));
		JavaRDD<?> ret = SparkCreateDeduplicationCandidates.duplicationCandidatePairsFromFingerprints(
				input,
				dedup
		);
		
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private static String minHashesDoc(String id, Integer...hashes) {
		return doc(id, Arrays.asList(hashes), Arrays.asList(1, 2, 3));
	}
	
	private static String simHashesDoc(String id, Integer...hashes) {
		return doc(id, Arrays.asList(1, 2, 3), Arrays.asList(hashes));
	}
	
	private static String doc(String id, List<Integer> minHashes, List<Integer> simHashes) {
		return "{\"docId\": \"" + id + "\",\"fingerprints\":{\"MinHashWithJavaHash\": " + minHashes +", \"64BitK3SimHashOneGramms\": "+ simHashes +"}}";
	}
}
