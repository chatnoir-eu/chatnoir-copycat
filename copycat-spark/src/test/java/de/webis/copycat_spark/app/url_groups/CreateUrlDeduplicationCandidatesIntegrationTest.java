package de.webis.copycat_spark.app.url_groups;

import java.net.URL;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.app.url_groups.CreateUrlDeduplicationCandidates;
import de.webis.copycat_spark.app.url_groups.DocumentForUrlDeduplication;
import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import lombok.SneakyThrows;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class CreateUrlDeduplicationCandidatesIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void testThatNoExactDuplicatesAreExtractedWhenCanonicalUrlsDoNotOverlap() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[0,0,0,0],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,0,0],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,0,0],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-3.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,0,0],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-4.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,0,0],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-5.com\"}"
		);
		
		List<String> actual = exactDuplicates(input);
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testThatNoExactDuplicatesAreWithinIdenticalUrls() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,1,0,0],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,1,0],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,0,1],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,1,0,0],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}"
		);
			
		List<String> actual = exactDuplicates(input);
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void testThatExactDuplicatesAreExtractedWithinSingleGroup() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,1,0],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}"
		);
		
		String expected = "{\"equivalentDocuments\": [\"id-1\",\"id-2\",\"id-4\",\"id-5\"],\"hash\":[920227876, -527890852, 1754043228, 1675872471, 1187455817, 1, 0, 0, 0]}";
		List<String> actual = exactDuplicates(input);
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals(expected, actual.get(0));
	}
	
	@Test
	public void testThatExactDuplicatesAreExtractedWithinTwoGroups() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[0,0,1,0],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,0,0,0],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}"
		);
		
		List<String> expected = Arrays.asList(
			"{\"equivalentDocuments\": [\"id-1\",\"id-5\"],\"hash\":[920227876, -527890852, 1754043228, 1675872471, 1187455817, 1, 0, 0, 0]}",
			"{\"equivalentDocuments\": [\"id-2\",\"id-4\"],\"hash\":[921151397, -398808133, 1883125947, 673676602, -634657969, 1, 0, 0, 0]}"
		);
		List<String> actual = exactDuplicates(input);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testCreationOfDeduplicationTasksOnTwoIdenticalGroups() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[5,6,7,8],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[9,10,11,1],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[12,13,14,15],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}"
		);
		
		List<String> expected = Arrays.asList(
			"{\"entries\":[{\"id\":\"id-1\",\"hashParts\":[1,2,3,4]},{\"id\":\"id-3\",\"hashParts\":[9,10,11,1]}]}"
		);
		List<String> actual = deduplicationTasks(input);
		Assert.assertEquals(expected, actual);
	}

	@Test
	public void testCreationOfDeduplicationTasksWithMultipleDuplicates() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[5,6,7,8],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[9,10,11,1],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[12,13,14,15],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-6\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-7\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-8\",\"canonicalURL\":\"http://group-1.com\"}"
		);
		
		List<String> expected = Arrays.asList(
			"{\"entries\":[{\"id\":\"id-1\",\"hashParts\":[1,2,3,4]},{\"id\":\"id-3\",\"hashParts\":[9,10,11,1]}]}"
		);
		List<String> actual = deduplicationTasks(input);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testCreationOfDeduplicationTasksWithMultipleGroups() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[5,6,7,12],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[9,10,11,1],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[12,13,14,30],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-6\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-7\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-8\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[21,22,12,24],\"docId\":\"id-9\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[21,22,12,24],\"docId\":\"id-10\",\"canonicalURL\":\"http://group-2.com\"}"
		);

		List<String> expected = Arrays.asList(
			"{\"entries\":[{\"id\":\"id-1\",\"hashParts\":[1,2,3,4]},{\"id\":\"id-3\",\"hashParts\":[9,10,11,1]}]}",
			"{\"entries\":[{\"id\":\"id-10\",\"hashParts\":[21,22,12,24]},{\"id\":\"id-2\",\"hashParts\":[5,6,7,12]},{\"id\":\"id-4\",\"hashParts\":[12,13,14,30]}]}"
		);
		List<String> actual = deduplicationTasks(input);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testCreationOfDeduplicationTasksWithMultipleTransitiveGroups() {
		JavaRDD<String> input = asRDD(
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-1\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[5,6,310,24],\"docId\":\"id-2\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[9,10,11,1],\"docId\":\"id-3\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[12,13,14,30],\"docId\":\"id-4\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-5\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-6\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[1,2,3,4],\"docId\":\"id-7\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[17,18,19,20],\"docId\":\"id-8\",\"canonicalURL\":\"http://group-1.com\"}",
			"{\"simHash64BitK3OneGramms\":[21,22,12,24],\"docId\":\"id-9\",\"canonicalURL\":\"http://group-2.com\"}",
			"{\"simHash64BitK3OneGramms\":[21,22,12,24],\"docId\":\"id-10\",\"canonicalURL\":\"http://group-2.com\"}"
		);

		List<String> expected = Arrays.asList(
			"{\"entries\":[{\"id\":\"id-1\",\"hashParts\":[1,2,3,4]},{\"id\":\"id-3\",\"hashParts\":[9,10,11,1]}]}",
			"{\"entries\":[{\"id\":\"id-10\",\"hashParts\":[21,22,12,24]},{\"id\":\"id-2\",\"hashParts\":[5,6,310,24]}]}",
			"{\"entries\":[{\"id\":\"id-10\",\"hashParts\":[21,22,12,24]},{\"id\":\"id-4\",\"hashParts\":[12,13,14,30]}]}"
		);
		List<String> actual = deduplicationTasks(input);
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	@SneakyThrows
	public void testRepresentationForExactDeduplication() {
		String expected = "{\"docId\":\"id-1\",\"url\":null,\"canonicalURL\":\"http://www.foo-bar.com\",\"fingerprints\":{\"pseudo-fingerprint\":[-927958794,1093489482,-1233280950,-1477603429,-376759062,0,0,0,0]},\"crawlingTimestamp\":null,\"documentLengthInWords\":0,\"documentLengthInWordsWithoutStopWords\":0}";
		DocumentForUrlDeduplication input = new DocumentForUrlDeduplication(Arrays.asList(0,0,0,0), "id-1", new URL("http://www.foo-bar.com"));
		String actual = CreateUrlDeduplicationCandidates.toDocumentRepresentationForExactHashDuplicatesWithSameUrlOrCanonicalUrl(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	@SneakyThrows
	public void testRepresentationForExactDeduplicationWithDifferentUrl() {
		String expected = "{\"docId\":\"id-2\",\"url\":null,\"canonicalURL\":\"http://www.foo-bar2.com\",\"fingerprints\":{\"pseudo-fingerprint\":[1300152258,2001030038,408267670,-1370092494,-923877332,1,1,1,1]},\"crawlingTimestamp\":null,\"documentLengthInWords\":0,\"documentLengthInWordsWithoutStopWords\":0}";
		DocumentForUrlDeduplication input = new DocumentForUrlDeduplication(Arrays.asList(1,1,1,1), "id-2", new URL("http://www.foo-bar2.com"));
		String actual = CreateUrlDeduplicationCandidates.toDocumentRepresentationForExactHashDuplicatesWithSameUrlOrCanonicalUrl(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> exactDuplicates(JavaRDD<String> input) {
		return CreateUrlDeduplicationCandidates.exactDuplicates(input, 1).collect().stream()
				.sorted()
				.collect(Collectors.toList());
	}
	
	private List<String> deduplicationTasks(JavaRDD<String> input) {
		return CreateUrlDeduplicationCandidates.createDeduplicationtasks(input, 1).collect().stream()
				.sorted()
				.collect(Collectors.toList());
	}
}
