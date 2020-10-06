package de.webis.cikm20_duplicates.app;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

public class FilterNearDuplicatesBetweenCorporaIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void testBetweenSingleCorpus() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:046c4670-e2cd-4c42-a658-7ad2576a769c>\",\"hemmingDistance\":1}",
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:09881804-7de8-451d-844b-e8e63254fb20>\",\"hemmingDistance\":1}",
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:176ebe21-7020-4a23-adfb-34f237dc273c>\",\"hemmingDistance\":2}",
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:2d59b2be-ac06-485e-ad4a-6f93c4bfba24>\",\"hemmingDistance\":2}"
		));
		
		List<String> actual = FilterNearDuplicatesBetweenCorpora.keepNearDuplicatesBetweenCorpora(input).collect().stream().sorted().collect(Collectors.toList());
		
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testBetweenSingleCorpus2() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:046c4670-e2cd-4c42-a658-7ad2576a769c>\",\"hemmingDistance\":1}",
			"{\"firstId\":\"clueweb09-xx\",\"secondId\":\"clueweb09-xy\",\"hemmingDistance\":1}",
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:176ebe21-7020-4a23-adfb-34f237dc273c>\",\"hemmingDistance\":2}",
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:2d59b2be-ac06-485e-ad4a-6f93c4bfba24>\",\"hemmingDistance\":2}"
		));
		
		List<String> actual = FilterNearDuplicatesBetweenCorpora.keepNearDuplicatesBetweenCorpora(input).collect().stream().sorted().collect(Collectors.toList());
		
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testBetweenMultipleCorpora() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:046c4670-e2cd-4c42-a658-7ad2576a769c>\",\"hemmingDistance\":1}",
			"{\"firstId\":\"clueweb09-xx\",\"secondId\":\"clueweb09-xy\",\"hemmingDistance\":1}",
			"{\"firstId\":\"<urn:uuid:022c04e1-6182-4f33-9ad1-d905ef776925>\",\"secondId\":\"<urn:uuid:176ebe21-7020-4a23-adfb-34f237dc273c>\",\"hemmingDistance\":2}",
			"{\"firstId\":\"clueweb09-xy\",\"secondId\":\"<urn:uuid:2d59b2be-ac06-485e-ad4a-6f93c4bfba24>\",\"hemmingDistance\":2}"
		));
		
		List<String> actual = FilterNearDuplicatesBetweenCorpora.keepNearDuplicatesBetweenCorpora(input).collect().stream().sorted().collect(Collectors.toList());
		List<String> expected = Arrays.asList("{\"firstId\":\"clueweb09-xy\",\"secondId\":\"<urn:uuid:2d59b2be-ac06-485e-ad4a-6f93c4bfba24>\",\"hemmingDistance\":2}");
		
		Assert.assertEquals(expected, actual);
	}
}
