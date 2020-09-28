package de.webis.cikm20_duplicates.spark.eval;

import java.util.Arrays;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

public class SparkCountS3EdgesAboveThresholdIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void nullValue() {
		Map<String, Long> thresholdToCount = SparkAnalyzeCanonicalLinkGraph.s3thresholdToEdgeCountNullValue();
		Approvals.verifyAsJson(thresholdToCount);
	}
	
	@Test
	public void additionOfValues() {
		Map<String, Long> a = SparkAnalyzeCanonicalLinkGraph.s3thresholdToEdgeCountNullValue();
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.43);
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.57);
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.431);
		
		Approvals.verifyAsJson(a);
	}
	
	@Test
	public void MergingOfValues() {
		Map<String, Long> a = SparkAnalyzeCanonicalLinkGraph.s3thresholdToEdgeCountNullValue();
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.11);
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.11212);
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.98);
		SparkAnalyzeCanonicalLinkGraph.add(a, 0.99);
		
		Map<String, Long> b = SparkAnalyzeCanonicalLinkGraph.s3thresholdToEdgeCountNullValue();
		SparkAnalyzeCanonicalLinkGraph.add(b, 0.11212);
		SparkAnalyzeCanonicalLinkGraph.add(b, 0.421);
		SparkAnalyzeCanonicalLinkGraph.add(b, 0.99099);
		
		Approvals.verifyAsJson(SparkAnalyzeCanonicalLinkGraph.merge(a,b));
	}
	
	@Test
	public void integrationOnSmallExample() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"s3score\": 0.67}",
			"{\"s3score\": 0.82}",
			"{\"s3score\": 0.6731}",
			"{\"s3score\": 0.820012}"
		));
		
		Map<String, Long> actual = SparkAnalyzeCanonicalLinkGraph.countEdgesAboveThreshold(input);

		Approvals.verifyAsJson(actual);
	}
}
