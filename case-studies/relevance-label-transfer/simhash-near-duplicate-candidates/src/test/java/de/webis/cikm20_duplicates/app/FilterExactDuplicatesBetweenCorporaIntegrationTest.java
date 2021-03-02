package de.webis.cikm20_duplicates.app;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

public class FilterExactDuplicatesBetweenCorporaIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void testThatExactPairsWithinSameCorporaAreNotRetrievedAtAll() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"equivalentDocuments\": [\"clueweb09-en0009-44-07911\",\"clueweb09-en0102-96-15951\",\"clueweb09-en0128-24-00228\"],\"hash\":[708313088, 7807, 2818254, 7237632]}",
			"{\"equivalentDocuments\": [\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"<urn:uuid:9f962c83-8f1d-43f0-a650-34bf285e516f>\",\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\"],\"hash\":[1089142784, 50345, 16515318, 7843584]}"
		));
		
		List<String> actual = exactDuplicatesBetweenCorporaWithJudgments(input);
		
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testThatPairsWithJudgedCw09DocumentsAreExtracted() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"equivalentDocuments\": [\"clueweb09-en0009-44-07911\",\"clueweb09-en0001-01-17957\",\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"clueweb12-my-test\",\"clueweb09-en0128-24-00228\"],\"hash\":[708313088, 7807, 2818254, 7237632]}",
			"{\"equivalentDocuments\": [\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"<urn:uuid:9f962c83-8f1d-43f0-a650-34bf285e516f>\",\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\"],\"hash\":[1089142784, 50345, 16515318, 7843584]}"
		));
		
		List<String> actual = exactDuplicatesBetweenCorporaWithJudgments(input);
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"clueweb09-en0001-01-17957\",\"secondId\":\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"hemmingDistance\":0}",
			"{\"firstId\":\"clueweb09-en0001-01-17957\",\"secondId\":\"clueweb12-my-test\",\"hemmingDistance\":0}"
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatPairsWithJudgedCw12DocumentsAreExtracted() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"equivalentDocuments\": [\"clueweb09-en0009-44-07911\",\"clueweb12-0000wb-60-01497\",\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"clueweb12-my-test\",\"clueweb09-en0128-24-00228\"],\"hash\":[708313088, 7807, 2818254, 7237632]}",
			"{\"equivalentDocuments\": [\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"<urn:uuid:9f962c83-8f1d-43f0-a650-34bf285e516f>\",\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\"],\"hash\":[1089142784, 50345, 16515318, 7843584]}"
		));
		
		List<String> actual = exactDuplicatesBetweenCorporaWithJudgments(input);
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"clueweb12-0000wb-60-01497\",\"secondId\":\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"hemmingDistance\":0}"
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testThatPairsWithJudgedCw09AndCw12DocumentsAreExtracted() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"equivalentDocuments\": [\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\",\"clueweb09-en0009-44-07911\",\"clueweb09-en0001-01-17957\",\"clueweb12-0000wb-60-01497\",\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"clueweb12-my-test\",\"clueweb09-en0128-24-00228\"],\"hash\":[708313088, 7807, 2818254, 7237632]}",
			"{\"equivalentDocuments\": [\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"<urn:uuid:9f962c83-8f1d-43f0-a650-34bf285e516f>\",\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\"],\"hash\":[1089142784, 50345, 16515318, 7843584]}"
		));
		
		List<String> actual = exactDuplicatesBetweenCorporaWithJudgments(input);
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"clueweb09-en0001-01-17957\",\"secondId\":\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"hemmingDistance\":0}",
			"{\"firstId\":\"clueweb09-en0001-01-17957\",\"secondId\":\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\",\"hemmingDistance\":0}",
			"{\"firstId\":\"clueweb09-en0001-01-17957\",\"secondId\":\"clueweb12-0000wb-60-01497\",\"hemmingDistance\":0}",
			"{\"firstId\":\"clueweb09-en0001-01-17957\",\"secondId\":\"clueweb12-my-test\",\"hemmingDistance\":0}",
			"{\"firstId\":\"clueweb12-0000wb-60-01497\",\"secondId\":\"<urn:uuid:7e5a5b19-b9b9-4835-8752-69cd1f6bcd06>\",\"hemmingDistance\":0}",
			"{\"firstId\":\"clueweb12-0000wb-60-01497\",\"secondId\":\"<urn:uuid:dd8b5fb8-80ba-4f6f-a078-ae294930b030>\",\"hemmingDistance\":0}"
		);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> exactDuplicatesBetweenCorporaWithJudgments(JavaRDD<String> input) {
		List<String> ret = FilterExactDuplicatesBetweenCorpora.exactDuplicatesBetweenCorporaWithJudgments(input).collect();
		ret = new ArrayList<>(ret);
		Collections.sort(ret);
		
		return ret;
	}
}
