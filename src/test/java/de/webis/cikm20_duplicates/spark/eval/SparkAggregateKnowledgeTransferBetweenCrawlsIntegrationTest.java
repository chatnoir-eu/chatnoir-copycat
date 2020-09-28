package de.webis.cikm20_duplicates.spark.eval;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;

public class SparkAggregateKnowledgeTransferBetweenCrawlsIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void checkThatMergingOfCountersWorks() {
		Map<String, Long> a = new HashMap<>();
		a.put("a_1", 10l);
		a.put("x_1", 10l);
		
		Map<String, Long> b = new HashMap<>();
		b.put("x_1", 11l);
		b.put("b_1", 11l);
		
		Map<String, Long> expected = new HashMap<>();
		expected.put("x_1", 21l);
		expected.put("b_1", 11l);
		expected.put("a_1", 10l);
		
		Map<String, Long> actual = SparkAggregateKnowledgeTransferBetweenCrawls.aggregate(a,b);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkLabelForClueWeb09ToClueWeb09Label() {
		List<String> expected = Collections.emptyList();
		List<String> actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb09-a", "clueweb09-b");

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkLabelForClueWeb09ToAllOthersLabel() {
		List<String> actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb09-a", "clueweb12-b");
		Assert.assertEquals(Arrays.asList("cw09-to-cw12"), actual);
		
		actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb12-b", "clueweb09-a");
		Assert.assertEquals(Arrays.asList("cw09-to-cw12"), actual);
		
		actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("b", "clueweb09-a");
		Assert.assertEquals(Arrays.asList("cw09-to-cc15"), actual);

		actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb09-a", "b");
		Assert.assertEquals(Arrays.asList("cw09-to-cc15"), actual);
	}
	
	@Test
	public void checkLabelForClueWeb012oAllOthersLabel() {
		List<String> actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb09-a", "clueweb12-b");
		Assert.assertEquals(Arrays.asList("cw09-to-cw12"), actual);
		
		actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb12-b", "clueweb09-a");
		Assert.assertEquals(Arrays.asList("cw09-to-cw12"), actual);
		
		actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("b", "clueweb12-a");
		Assert.assertEquals(Arrays.asList("cw12-to-cc15"), actual);

		actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb12-a", "b");
		Assert.assertEquals(Arrays.asList("cw12-to-cc15"), actual);
	}
	
	@Test
	public void labelsForIntegrationExample1() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"clueweb09-1911wb-61-32607,clueweb12-1910wb-90-00004",
				"clueweb12-1911wb-63-21940,clueweb12-1911wb-61-32607",
				"clueweb12-1912wb-69-16845,clueweb12-1912wb-23-02148",
				"clueweb12-1913wb-21-14719,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-21-14723,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-34-21369,clueweb12-1913wb-11-13833"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"clueweb12-0310wb-12-31094\",\"clueweb12-0309wb-77-14169\",\"clueweb09-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"clueweb12-0504wb-14-32223\",\"clueweb12-0308wb-11-17456\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		Map<String, Long> expected = new HashMap<>();
		expected.put("cw09-to-cw12", 4l);
		
		Map<String, Long> actual = aggregateKnowledgeTransfer(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void labelsForIntegrationExample2() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"clueweb09-1911wb-61-32607,clueweb12-1910wb-90-00004",
				"cluseweb12-1911wb-63-21940,clueweb12-1911wb-61-32607",
				"cluewseb12-1912wb-69-16845,clueweb12-1912wb-23-02148",
				"clueweb12-1913wb-21-14719,cluewes12-1912wb-97-19250",
				"clueweb12-1913wb-21-14723,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-34-21369,clueweb12-1913wb-11-13833"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"sas-0310wb-12-31094\",\"sa-0309wb-77-14169\",\"clueweb09-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"clueweb12-0504wb-14-32223\",\"clueweb12-0308wb-11-17456\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		Map<String, Long> expected = new HashMap<>();
		expected.put("cw09-to-cw12", 2l);
		expected.put("cw09-to-cc15", 2l);
		expected.put("cw12-to-cc15", 3l);
		
		
		Map<String, Long> actual = aggregateKnowledgeTransfer(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void labelsForIntegrationExample3() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"clueweb09-en0008-02-29970,clueweb12-1910wb-90-00004",
				"cluseweb12-1911wb-63-21940,clueweb12-1911wb-61-32607",
				"cluewseb12-1912wb-69-16845,clueweb12-1912wb-23-02148",
				"clueweb12-1913wb-21-14719,cluewes12-1912wb-97-19250",
				"clueweb12-1913wb-21-14723,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-34-21369,clueweb12-1913wb-11-13833"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"sas-0310wb-12-31094\",\"sa-0309wb-77-14169\",\"clueweb09-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"clueweb12-0504wb-14-32223\",\"clueweb12-0308wb-11-17456\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		Map<String, Long> expected = new HashMap<>();
		expected.put("cw09-to-cw12", 2l);
		expected.put("cw09-to-cc15", 2l);
		expected.put("cw12-to-cc15", 3l);
		expected.put("cw09-to-cw12---topic---CLUEWEB09::WEB_2009::50---source-doc---clueweb09-en0008-02-29970---relevance---0", 1l);
		
		Map<String, Long> actual = aggregateKnowledgeTransfer(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void labelsForIntegrationExample4() {
		List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled = Arrays.asList(
				"clueweb09-en0008-02-29970,74a36712-7671-55f6-8ece-1714d810a3c2",
				"cluseweb12-1911wb-63-21940,clueweb12-1911wb-61-32607",
				"cluewseb12-1912wb-69-16845,clueweb12-1912wb-23-02148",
				"clueweb12-1913wb-21-14719,cluewes12-1912wb-97-19250",
				"clueweb12-1913wb-21-14723,clueweb12-1912wb-97-19250",
				"clueweb12-1913wb-34-21369,clueweb12-1913wb-11-13833"
			);
			
		List<String> exampleExactDuplicatesShuffled = Arrays.asList(
				"{\"equivalentDocuments\": [\"sas-0310wb-12-31094\",\"sa-0309wb-77-14169\",\"clueweb09-0309wb-77-14169\"],\"hash\":[2143027200, 11380, 15597713, 679168]}",
				"{\"equivalentDocuments\": [\"clueweb12-0504wb-14-32223\",\"clueweb12-0308wb-11-17456\"],\"hash\":[-729088000, 64915, 1310914, 3330304]}",
				"{\"equivalentDocuments\": [\"clueweb12-0405wb-75-31987\",\"clueweb09-en0077-45-03495\"],\"hash\":[-1076559872, 25846, 7208991, 16488448]}"
			); 
		
		Map<String, Long> expected = new HashMap<>();
		expected.put("cw09-to-cw12", 1l);
		expected.put("cw09-to-cc15", 3l);
		expected.put("cw12-to-cc15", 3l);
		expected.put("cw09-to-cc15---topic---CLUEWEB09::WEB_2009::50---source-doc---clueweb09-en0008-02-29970---relevance---0", 1l);
		
		Map<String, Long> actual = aggregateKnowledgeTransfer(exampleNearDuplicatesWithoutExactDuplicatesShuffled, exampleExactDuplicatesShuffled);
		
		Assert.assertEquals(expected, actual);
	}
	
	private Map<String, Long> aggregateKnowledgeTransfer(List<String> exampleNearDuplicatesWithoutExactDuplicatesShuffled, List<String> exampleExactDuplicatesShuffled) {
		JavaRDD<String> a = jsc().parallelize(exampleNearDuplicatesWithoutExactDuplicatesShuffled);
		JavaRDD<String> b = jsc().parallelize(exampleExactDuplicatesShuffled);
		
		return SparkAggregateKnowledgeTransferBetweenCrawls.aggregateKnowledgeTransfer(a,b);
	}

	@Test
	public void checkLabelForClueWeb12ToClueWeb12Label() {
		List<String> expected = Collections.emptyList();
		List<String> actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("clueweb12-a", "clueweb12-b");

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void checkLabelForCCToCCLabel() {
		List<String> expected = Collections.emptyList();
		List<String> actual = SparkAggregateKnowledgeTransferBetweenCrawls.labels("a", "b");

		Assert.assertEquals(expected, actual);
	}
}
