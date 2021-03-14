package de.webis.cikm20_duplicates.spark.eval;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkCreateSourceDocumentsIntegrationTest;
import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

public class SparkSampleS3EdgesPerBinIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void approveAllBins() {
		List<String> expected = Arrays.asList("0.4000", "0.4500", "0.5000",
			"0.5500", "0.6000", "0.6500", "0.7000", "0.7500", "0.8000",
			"0.8500", "0.9000", "0.9500"
		);
		List<String> actual = SparkSampleS3EdgesPerBin.bins();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void detectLargestBin1() {
		String expected = "0.9500";
		String actual = SparkSampleS3EdgesPerBin.bin("{\"s3score\": 0.960012}");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void detectLargestBin2() {
		String expected = "0.9500";
		String actual = SparkSampleS3EdgesPerBin.bin("{\"s3score\": 10}");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void detectSampleBin() {
		String expected = "0.7000";
		String actual = SparkSampleS3EdgesPerBin.bin("{\"s3score\": 0.712}");
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void integrationOnSmallExample() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"s3score\": 0.67, \"tmp\": 2}",
			"{\"s3score\": 0.82, \"tmp\": 2}",
			"{\"s3score\": 0.6731, \"tmp\": 2}",
			"{\"s3score\": 0.820012, \"tmp\": 2}"
		));
		
		List<String> actual = SparkCreateSourceDocumentsIntegrationTest.sorted(SparkSampleS3EdgesPerBin.samplePerBin(input));

		Approvals.verifyAsJson(actual);
	}
}
