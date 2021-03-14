package de.webis.cikm20_duplicates.spark;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

public class SparkCombineIntermediateResultstIntegrationTest extends SparkIntegrationTestBase {

	@Test(expected = Exception.class)
	public void testCheckExceptionIsThrownForMissingFirstId() {
		combineIntermediateResults(
				"{\"firsstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}"
		);
	}
	
	@Test(expected = Exception.class)
	public void testCheckExceptionIsThrownForMissingSecondId() {
		combineIntermediateResults(
				"{\"firstId\":\"clueweb09-es0014-01-08083\",\"seccondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}"
		);
	}
	
	@Test(expected = Exception.class)
	public void testCheckExceptionIsThrownForMissingHemmingDistance() {
		combineIntermediateResults(
				"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemsmingDistance\":2}"
		);
	}
	
	@Test
	public void testCombinationOfEmptyResults() {
		List<String> expected = Arrays.asList();
		List<String> actual = combineIntermediateResults();
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testCombinationOfSmallSampleInput() {
		List<String> expected = Arrays.asList(
			"clueweb09-es0014-01-08083,clueweb09-es0021-46-13605,2",
			"clueweb09-es0014-03-19697,clueweb09-pt0001-81-12710,3"
		);
		List<String> actual = combineIntermediateResults(
			"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}"
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testCombinationOfSmallSampleInputWithEmptyStrings() {
		List<String> expected = Arrays.asList(
			"clueweb09-es0014-01-08083,clueweb09-es0021-46-13605,2",
			"clueweb09-es0014-03-19697,clueweb09-pt0001-81-12710,3"
		);
		List<String> actual = combineIntermediateResults(
			"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}",
			"        ",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}",
			"\t"
		);

		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testCombinationOfSmallSampleInputWithDuplicates() {
		List<String> expected = Arrays.asList(
			"clueweb09-es0014-01-08083,clueweb09-es0021-46-13605,2",
			"clueweb09-es0014-03-19697,clueweb09-pt0001-81-12710,3"
		);
		List<String> actual = combineIntermediateResults(
			"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}",
			"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}"
		);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testCombinationOfSmallSampleInputWithDuplicatesAndOtherOrder() {
		List<String> expected = Arrays.asList(
			"clueweb09-es0014-01-08083,clueweb09-es0021-46-13605,2",
			"clueweb09-es0014-03-19697,clueweb09-pt0001-81-12710,3"
		);
		List<String> actual = combineIntermediateResults(
				"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}",
			"{\"firstId\":\"clueweb09-es0014-03-19697\",\"secondId\":\"clueweb09-pt0001-81-12710\",\"hemmingDistance\":3}",
			"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}",
			"{\"firstId\":\"clueweb09-es0014-01-08083\",\"secondId\":\"clueweb09-es0021-46-13605\",\"hemmingDistance\":2}"	
		);

		Assert.assertEquals(expected, actual);
	}
	
	private List<String> combineIntermediateResults(String...input) {
		JavaRDD<String> inputRDD = jsc().parallelize(Arrays.asList(input));
		JavaRDD<String> ret = SparkCombineIntermediateResults.combineIntermediateResults(inputRDD, 1);
		
		return ret.collect();
	}
}
