package de.webis.cikm20_duplicates.app.url_groups;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

import de.webis.cikm20_duplicates.spark.SparkIntegrationTestBase;
import lombok.SneakyThrows;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class UrlBasedRepartitioningOfDocumentsIntegrationTest extends SparkIntegrationTestBase {
	@Test
	public void approveRepartitioningOnSampleWith7Documents() {
		JavaRDD<String> input = asRDD(
			"{\"docId\":\"id-1\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-1.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[0,0,0,0],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-2\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-1.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[1,1,1,1],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-3\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-2.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[2,2,2,2],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-4\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-2.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[3,3,3,3],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-5\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-3.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[4,4,4,4],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-6\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-4.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[5,5,5,5],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-7\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-4.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[6,6,6,6],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}"
		);
		
		List<String> actual = mapToPartitions(input); 
		Approvals.verifyAsJson(actual);
	}

	@Test
	public void approveRepartitioningOnSampleWith7DocumentsInShuffledOrder() {
		JavaRDD<String> input = asRDD(
			"{\"docId\":\"id-1\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-1.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[0,0,0,0],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-2\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-1.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[1,1,1,1],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-3\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-2.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[2,2,2,2],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-4\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-2.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[3,3,3,3],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-5\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-3.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[4,4,4,4],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-6\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-4.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[5,5,5,5],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}",
			"{\"docId\":\"id-7\",\"url\":\"http://does-not-exist.com\",\"canonicalURL\":\"http://group-4.com\",\"fingerprints\":{\"64BitK3SimHashOneGramms\":[6,6,6,6],\"64BitK3SimHashThreeAndFiveGramms\":[-1,-1,-1,-1]}}"
		);
		
		List<String> actual = mapToPartitions(input); 
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void testDeserializationOfDocumentForUrlDeduplication() throws Exception {
		String input = "{\"docId\":\"my-id\", \"canonicalURL\": \"http://foo-bar.com\"}";
		DocumentForUrlDeduplication expected = new DocumentForUrlDeduplication(null, "my-id", new URL("http://foo-bar.com"));
		DocumentForUrlDeduplication actual = DocumentForUrlDeduplication.fromString(input);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> mapToPartitions(JavaRDD<String> input) {
		JavaRDD<String> ret = UrlBasedRepartitioningOfDocuments.repartitionDocumentsWithTheSameUrlOrCanonicalUrlToTheSamePart(input, 10);
		ret = ret.mapPartitions(i -> collectPartition(i));
		
		return ret.collect()
				.stream()
				.sorted()
				.collect(Collectors.toList());
	}

	@SneakyThrows
	private static Iterator<String> collectPartition(Iterator<String> i) {
		List<String> ret = ImmutableList.copyOf(i);
		return new ArrayList<>(Arrays.asList(new ObjectMapper().writeValueAsString(ret))).iterator();
	}
}
