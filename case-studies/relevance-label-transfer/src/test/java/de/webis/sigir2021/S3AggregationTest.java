package de.webis.sigir2021;

import java.io.File;
import java.util.Arrays;
import java.util.List;

import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.S3Aggregation;

public class S3AggregationTest {
	@Test
	public void testEmptyLists() {
		List<File> input = Arrays.asList();
		String actual = S3Aggregation.countS3SimilaritiesCw09ToCw12(input);
		
		Approvals.verify(actual);
	}
	
	@Test
	public void testCw09ToCW12Similarities() {
		List<File> input = Arrays.asList(new File("src/test/resources/small-web-2009-sample.jsonl"));
		String actual = S3Aggregation.countS3SimilaritiesCw09ToCw12(input);
		
		Approvals.verify(actual);
	}
	
	@Test
	public void testCw09ToCW12SimilaritiesWithMultipleFiles() {
		List<File> input = Arrays.asList(new File("src/test/resources/small-web-2009-sample.jsonl"), new File("src/test/resources/small-web-2012-sample.jsonl"));
		String actual = S3Aggregation.countS3SimilaritiesCw09ToCw12(input);
		
		Approvals.verify(actual);
	}
	
	@Test
	public void testCw09ToCW12WaybackSimilaritiesWithMultipleFiles() {
		List<File> input = Arrays.asList(new File("src/test/resources/small-web-2009-sample.jsonl"), new File("src/test/resources/small-web-2012-sample.jsonl"));
		File waybackFile = new File("src/test/resources/sample-wayback-similarities.jsonl");
		String actual = S3Aggregation.countS3SimilaritiesCw09ToCw12AndWayback(input, waybackFile);
		
		Approvals.verify(actual);
	}
	
	@Test
	public void testCc15WithSingleFile() {
		List<File> input = Arrays.asList(new File("src/test/resources/small-web-cc-2010.jsonl"));
		String actual = S3Aggregation.countS3SimilaritiesCc15(input);
		
		Approvals.verify(actual);
	}
	
	@Test
	public void assignBucketFor0_0() {
		String expected = "0.0000";
		double input = 0.0;
		String actual = S3Aggregation.bucket(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void assignBucketFor0_2() {
		String expected = "0.2000";
		double input = 0.201;
		String actual = S3Aggregation.bucket(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void assignBucketFor0_9352() {
		String expected = "0.9350";
		double input = 0.9352;
		String actual = S3Aggregation.bucket(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void assignBucketFor12() {
		String expected = "0.9950";
		double input = 12;
		String actual = S3Aggregation.bucket(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	// /mnt/ceph/storage/data-in-progress/kibi9872/sigir2021/data-13-10-2020/wayback-similarities.jsonl 
}
