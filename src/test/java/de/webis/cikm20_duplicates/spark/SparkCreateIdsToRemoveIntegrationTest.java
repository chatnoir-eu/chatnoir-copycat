package de.webis.cikm20_duplicates.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkCreateIdsToRemove.KeepId;

public class SparkCreateIdsToRemoveIntegrationTest extends SparkIntegrationTestBase {
	private List<String> exampleNearDuplicatesWithoutExactDuplicates = Arrays.asList(
		"{\"firstId\":\"clueweb12-1910wb-90-00004\",\"secondId\":\"clueweb12-1911wb-61-32607\",\"hemmingDistance\":3}",
		"{\"firstId\":\"clueweb12-1911wb-61-32607\",\"secondId\":\"clueweb12-1911wb-63-21940\",\"hemmingDistance\":3}",
		"{\"firstId\":\"clueweb12-1912wb-23-02148\",\"secondId\":\"clueweb12-1912wb-69-16845\",\"hemmingDistance\":2}",
		"{\"firstId\":\"clueweb12-1912wb-97-19250\",\"secondId\":\"clueweb12-1913wb-21-14719\",\"hemmingDistance\":3}",
		"{\"firstId\":\"clueweb12-1912wb-97-19250\",\"secondId\":\"clueweb12-1913wb-21-14723\",\"hemmingDistance\":2}",
		"{\"firstId\":\"clueweb12-1913wb-11-13833\",\"secondId\":\"clueweb12-1913wb-34-21369\",\"hemmingDistance\":1}"
	);
	
	private List<String> exampleExactDuplicates = Arrays.asList(
		"{\"equivalentDocuments\": [\"clueweb12-0309wb-77-14169\",\"clueweb12-0310wb-12-31094\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
		"{\"equivalentDocuments\": [\"clueweb12-0308wb-11-17456\",\"clueweb12-0504wb-14-32223\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
		"{\"equivalentDocuments\": [\"clueweb09-en0077-45-03495\",\"clueweb12-0405wb-75-31987\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
	); 
	
	@Test
	public void ApproveIdsToRemoveForCw12() {
		List<String> idsToRemove = idsToRemove(exampleNearDuplicatesWithoutExactDuplicates, exampleExactDuplicates, SparkCreateIdsToRemove.CLUEWEB12);
		Approvals.verifyAsJson(idsToRemove);
	}

	@Test
	public void ApproveIdsToRemoveForCw09() {
		List<String> expected = Collections.emptyList();
		List<String> idsToRemove = idsToRemove(exampleNearDuplicatesWithoutExactDuplicates, exampleExactDuplicates, SparkCreateIdsToRemove.CLUEWEB09);
		
		Assert.assertEquals(expected, idsToRemove);
	}
	
	@Test
	public void ApproveIdsToRemoveForCw12WithShuffledStuIds() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"{\"firstId\":\"clueweb12-1911wb-61-32607\",\"secondId\":\"clueweb12-1910wb-90-00004\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1911wb-63-21940\",\"secondId\":\"clueweb12-1911wb-61-32607\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1912wb-69-16845\",\"secondId\":\"clueweb12-1912wb-23-02148\",\"hemmingDistance\":2}",
				"{\"firstId\":\"clueweb12-1913wb-21-14719\",\"secondId\":\"clueweb12-1912wb-97-19250\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1913wb-21-14723\",\"secondId\":\"clueweb12-1912wb-97-19250\",\"hemmingDistance\":2}",
				"{\"firstId\":\"clueweb12-1913wb-34-21369\",\"secondId\":\"clueweb12-1913wb-11-13833\",\"hemmingDistance\":1}"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"clueweb12-0310wb-12-31094\",\"clueweb12-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"clueweb12-0504wb-14-32223\",\"clueweb12-0308wb-11-17456\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		List<String> idsToRemove = idsToRemove(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled, SparkCreateIdsToRemove.CLUEWEB12);
		Approvals.verifyAsJson(idsToRemove);
	}
	
	@Test
	public void ApproveIdsToRemoveForCw12WithCsvFormat() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"clueweb12-1911wb-61-32607,clueweb12-1910wb-90-00004",
				"clueweb12-1911wb-63-21940,clueweb12-1911wb-61-32607",
				"clueweb12-1912wb-69-16845,clueweb12-1912wb-23-02148",
				"clueweb12-1913wb-21-14719,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-21-14723,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-34-21369,clueweb12-1913wb-11-13833"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"clueweb12-0310wb-12-31094\",\"clueweb12-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"clueweb12-0504wb-14-32223\",\"clueweb12-0308wb-11-17456\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		List<String> idsToRemove = idsToRemove(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled, SparkCreateIdsToRemove.CLUEWEB12);
		Approvals.verifyAsJson(idsToRemove);
	}
	
	@Test
	public void ApproveIdsToRemoveForCc() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"{\"firstId\":\"id1\",\"secondId\":\"id2\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1911wb-63-21940\",\"secondId\":\"clueweb12-1911wb-61-32607\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1912wb-69-16845\",\"secondId\":\"clueweb12-1912wb-23-02148\",\"hemmingDistance\":2}",
				"{\"firstId\":\"clueweb12-1913wb-21-14719\",\"secondId\":\"clueweb12-1912wb-97-19250\",\"hemmingDistance\":3}",
				"{\"firstId\":\"id0\",\"secondId\":\"id2\",\"hemmingDistance\":2}",
				"{\"firstId\":\"clueweb12-1913wb-34-21369\",\"secondId\":\"clueweb12-1913wb-11-13833\",\"hemmingDistance\":1}"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"clueweb12-0310wb-12-31094\",\"clueweb12-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"id30\",\"clueweb12-0308wb-11-17456\",\"id40\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		List<String> expected = Arrays.asList("id2", "id40");
		List<String> actual = idsToRemove(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled, SparkCreateIdsToRemove.COMMON_CRAWL);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkIdsBetweenCrawlsAreRemovedFor() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"{\"firstId\":\"id1\",\"secondId\":\"id2\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1911wb-63-21940\",\"secondId\":\"clueweb12-1911wb-61-32607\",\"hemmingDistance\":3}",
				"{\"firstId\":\"id0\",\"secondId\":\"id2\",\"hemmingDistance\":2}"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"id30\",\"clueweb12-0308wb-11-17456\",\"id40\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}"
			); 
		
		List<String> expected = Arrays.asList("clueweb12-1911wb-63-21940", "id2", "id30", "id40");
		List<String> actual = idsToRemove(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled, SparkCreateIdsToRemove.ALL_CRAWLS);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkIdsFromFile() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"{\"firstId\":\"id1\",\"secondId\":\"id2\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1911wb-63-21940\",\"secondId\":\"clueweb12-1911wb-61-32607\",\"hemmingDistance\":3}",
				"{\"firstId\":\"id0\",\"secondId\":\"id2\",\"hemmingDistance\":2}"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"id30\",\"clueweb12-0308wb-11-17456\",\"id40\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}"
			); 
		
		List<String> expected = Arrays.asList("id40");
		List<String> actual = idsToRemove(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled, SparkCreateIdsToRemove.idsToKeepFromFile("src/test/resources/data/list-with-pseudo-document-ids"));
		
		Assert.assertEquals(expected, actual);
	}
	

	@Test
	public void checkIdsFromFileWithCW12Prefilter() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"{\"firstId\":\"id1\",\"secondId\":\"id2\",\"hemmingDistance\":3}",
				"{\"firstId\":\"clueweb12-1911wb-63-21940\",\"secondId\":\"clueweb12-1911wb-61-32607\",\"hemmingDistance\":3}",
				"{\"firstId\":\"id0\",\"secondId\":\"id2\",\"hemmingDistance\":2}"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"id30\",\"clueweb12-0308wb-11-17456\",\"id40\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}"
			); 
		
		List<String> expected = Arrays.asList();
		List<String> actual = idsToRemove(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled, SparkCreateIdsToRemove.idsToKeepFromFile(SparkCreateIdsToRemove.CLUEWEB12, "src/test/resources/data/list-with-pseudo-document-ids"));
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> idsToRemove(List<String> nearDuplicates, List<String> exactDuplicates, KeepId keepId) {
		JavaRDD<String> a = jsc().parallelize(nearDuplicates);
		JavaRDD<String> b = jsc().parallelize(exactDuplicates);
		List<String> ret = new ArrayList<>(SparkCreateIdsToRemove.idsToRemove(a, b, keepId).collect());
		Collections.sort(ret);
		
		return ret;
	}
}
