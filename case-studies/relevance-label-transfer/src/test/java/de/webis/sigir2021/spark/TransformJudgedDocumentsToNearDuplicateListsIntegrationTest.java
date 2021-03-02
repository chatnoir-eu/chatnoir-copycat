package de.webis.sigir2021.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.spark.TransformJudgedDocumentsToNearDuplicateLists;

public class TransformJudgedDocumentsToNearDuplicateListsIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void testWithFewLines() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"sourceId\":\"clueweb09-en0003-78-24787\",\"targetIds\":[\"clueweb12-0005wb-07-18569\"]}",
			"{\"sourceId\":\"clueweb09-en0001-39-15474\",\"targetIds\":[\"clueweb12-0003wb-43-02583\"]}",
			"{\"sourceId\":\"clueweb09-en0010-33-25642\",\"targetIds\":[\"clueweb12-0712wb-51-09361\"]}"
		));
		List<String> expected = Arrays.asList(
				"{\"firstId\":\"clueweb09-en0001-39-15474\",\"secondId\":\"clueweb12-0003wb-43-02583\",\"hemmingDistance\":-1}",
				"{\"firstId\":\"clueweb09-en0003-78-24787\",\"secondId\":\"clueweb12-0005wb-07-18569\",\"hemmingDistance\":-1}",
				"{\"firstId\":\"clueweb09-en0010-33-25642\",\"secondId\":\"clueweb12-0712wb-51-09361\",\"hemmingDistance\":-1}"
		);
		
		List<String> actual = toDuplicatePairs(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testWithMoreLines() {
		JavaRDD<String> input = jsc().parallelize(Arrays.asList(
			"{\"sourceId\":\"clueweb09-en0003-78-24787\",\"targetIds\":[\"clueweb12-0005wb-07-18569\"]}",
			"{\"sourceId\":\"clueweb09-en0001-39-15474\",\"targetIds\":[\"clueweb12-0003wb-43-02583\"]}",
			"{\"sourceId\":\"clueweb09-en0010-33-25642\",\"targetIds\":[\"clueweb12-0712wb-51-09361\"]}",
			"{\"sourceId\":\"clueweb09-en0009-90-15077\",\"targetIds\":[\"clueweb12-0205wb-98-07545\"]}",
			"{\"sourceId\":\"clueweb09-en0006-83-16457\",\"targetIds\":[\"clueweb12-0500wb-50-05536\",\"clueweb12-0105wb-16-25749\"]}"
		));
		List<String> expected = Arrays.asList(
			"{\"firstId\":\"clueweb09-en0001-39-15474\",\"secondId\":\"clueweb12-0003wb-43-02583\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0003-78-24787\",\"secondId\":\"clueweb12-0005wb-07-18569\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0006-83-16457\",\"secondId\":\"clueweb12-0105wb-16-25749\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0006-83-16457\",\"secondId\":\"clueweb12-0500wb-50-05536\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0009-90-15077\",\"secondId\":\"clueweb12-0205wb-98-07545\",\"hemmingDistance\":-1}",
			"{\"firstId\":\"clueweb09-en0010-33-25642\",\"secondId\":\"clueweb12-0712wb-51-09361\",\"hemmingDistance\":-1}"
		);
		
		List<String> actual = toDuplicatePairs(input);
		
		Assert.assertEquals(expected, actual);
	}


	private List<String> toDuplicatePairs(JavaRDD<String> input) {
		List<String> ret = new ArrayList<>(TransformJudgedDocumentsToNearDuplicateLists.toDuplicatePairs(input).collect());
		Collections.sort(ret);
		
		return ret;
	}
}
