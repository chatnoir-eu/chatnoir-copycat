package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import com.holdenkarau.spark.testing.SharedJavaSparkContext;

public class SparkCountEdgeLabelsIntegrationTest extends SharedJavaSparkContext {

	@Test
	public void testWithEmptyExactDuplicatesAndPairs() {
		List<String> expected = Collections.emptyList();
		List<String> actual = countAllEdges(rdd(), rdd());
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithSomeClueWeb09DuplicatesAndEmptyPairs() {
		List<String> actual = countAllEdges(rdd(
				"{\"equivalentDocuments\": [\"clueweb09-1\",\"clueweb09-3\"],\"hash\":[]}",
				"{\"equivalentDocuments\": [\"clueweb09-2\",\"clueweb09-4\",\"clueweb09-5\",\"clueweb09-6\"],\"hash\":[]}"
		), rdd());
		
		//1 + 6 = 7
		Approvals.verify(actual);
	}
	
	@Test
	public void testWithSomeClueWeb12DuplicatesAndEmptyPairs() {
		List<String> actual = countAllEdges(rdd(
				"{\"equivalentDocuments\": [\"clueweb12-1\",\"clueweb12-3\",\"clueweb12-3\"],\"hash\":[]}"
		), rdd());
		
		//3
		Approvals.verify(actual);
	}
	
	@Test
	public void testWithSomeClueWeb09And12DuplicatesAndEmptyPairs() {
		List<String> actual = countAllEdges(rdd(
				"{\"equivalentDocuments\": [\"clueweb09-2\",\"clueweb12-4\",\"clueweb09-5\",\"clueweb09-6\"],\"hash\":[]}"
		), rdd());
		
		//3 ; 3 ; 0
		Approvals.verify(actual);
	}
	
	@Test
	public void testWithSomeClueWeb09And12DuplicatesAndSomePairs() {
		List<String> actual = countAllEdges(rdd(
				"{\"equivalentDocuments\": [\"clueweb09-2\",\"clueweb12-4\",\"clueweb09-5\",\"clueweb09-6\"],\"hash\":[]}"
		), rdd(
				"clueweb09-2,clueweb09-3,2",
				"clueweb09-2,clueweb09-3,2",
				"clueweb09-2,clueweb09-3,1",
				"clueweb12-2,clueweb09-3,3",
				"clueweb09-2,clueweb12-3,3",
				"clueweb12-2,clueweb12-3,1"
		));
		
		Approvals.verify(actual);
	}
	
	@Test
	public void testWithSomeClueWeb09And12DuplicatesAndSomePairsAndSomeJudgedDocumentsInPairs() {
		List<String> actual = countAllEdges(rdd(
				"{\"equivalentDocuments\": [\"clueweb09-2\",\"clueweb12-4\",\"clueweb09-5\",\"clueweb09-6\"],\"hash\":[]}"
		), rdd(
				"clueweb09-2,clueweb09-3,2",
				"clueweb09-2,clueweb09-3,2",
				"clueweb09-2,clueweb09-3,1",
				"clueweb12-2,clueweb09-3,3",
				"clueweb09-2,clueweb12-3,3",
				"clueweb12-2,clueweb12-3,1",
				"clueweb09-3,clueweb09-en0008-02-29970,1", //relevant
				"clueweb09-en0008-02-29970,clueweb09-04,2", //relevant
				"clueweb12-02,clueweb09-en0008-02-29970,3", //relevant
				"clueweb12-1800tw-04-16339,clueweb09-3,3" //relevant
		));
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testWithSomeClueWeb09And12DuplicatesAndEmptyPairsAndSomeJudgedDocuments() {
		List<String> actual = countAllEdges(rdd(
				"{\"equivalentDocuments\": [\"clueweb09-2\",\"clueweb12-1800tw-04-16339\",\"clueweb09-5\",\"clueweb09-en0008-02-29970\"],\"hash\":[]}"
		), rdd());
		
		//3 ; 3 ; 0
		Approvals.verifyAsJson(actual);
	}
	
	private List<String> countAllEdges(JavaRDD<String> exactDuplicates, JavaRDD<String> pairs) {
		JavaRDD<String> ret = SparkCountEdgeLabels.countEdgeLabels(exactDuplicates, pairs);
		return SparkCreateSourceDocumentsIntegrationTest.sorted(ret);
	}
	
	private JavaRDD<String> rdd(String...vals) {
		return jsc().parallelize(Arrays.asList(vals));
	}
}
