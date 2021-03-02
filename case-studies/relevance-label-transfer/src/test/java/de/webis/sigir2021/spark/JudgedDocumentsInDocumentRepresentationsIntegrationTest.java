package de.webis.sigir2021.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import de.webis.sigir2021.spark.JudgedDocumentsInDocumentRepresentations;

public class JudgedDocumentsInDocumentRepresentationsIntegrationTest extends SparkIntegrationTestBase {
	
	@Test
	public void approveURLSearchForEmptySourceAndEmptyTarget() {
		Map<String, String> docIdToUrl = new HashMap<>();
		JavaRDD<String> documentRepresentations = emptyDocuments();
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void approveURLSearchForEmptySourceAndTargetsWithoutCanonicalUrl() {
		Map<String, String> docIdToUrl = new HashMap<>();
		JavaRDD<String> documentRepresentations = testDocumentsWithoutCanonicalLinks();
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void approveURLSearchForNonOverlappingSourceWithTarget() {
		Map<String, String> docIdToUrl = new HashMap<>();
		docIdToUrl.put("id-1", "http://google.com/");
		docIdToUrl.put("id-2", "http://google.de/");
		JavaRDD<String> documentRepresentations = testDocumentsWithoutCanonicalLinks();
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertTrue(actual.isEmpty());
	}
	
	@Test
	public void approveURLSearchURLOverlap() {
		Map<String, String> docIdToUrl = new HashMap<>();
		docIdToUrl.put("id-1", "http://google.com/");
		docIdToUrl.put("id-2", "http://google.de/");
		docIdToUrl.put("id-3", "http://www.mtwain.com/Roughing_It/60.html");
		JavaRDD<String> documentRepresentations = testDocumentsWithoutCanonicalLinks();
		List<String> expected = Arrays.asList(
			"{\"sourceId\":\"id-3\",\"targetIds\":[\"clueweb12-0208wb-76-13634\"]}"
		);
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveURLSearchURLOverlapFuzzy() {
		Map<String, String> docIdToUrl = new HashMap<>();
		docIdToUrl.put("id-1", "http://google.com/");
		docIdToUrl.put("id-2", "http://google.de/");
		docIdToUrl.put("id-3", "https://www.mtwain.com/Roughing_It/60.html");
		JavaRDD<String> documentRepresentations = testDocumentsWithoutCanonicalLinks();
		List<String> expected = Arrays.asList(
			"{\"sourceId\":\"id-3\",\"targetIds\":[\"clueweb12-0208wb-76-13634\"]}"
		);
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveURLSearchEncoding1() {
		Map<String, String> docIdToUrl = new HashMap<>();
		docIdToUrl.put("id-1", "http://google.com/");
		docIdToUrl.put("id-2", "http://google.de/");
		docIdToUrl.put("id-3", "http://en.wikipedia.org/wiki/Biloxi,%20Mississippi");
		JavaRDD<String> documentRepresentations = testDocumentsWithoutCanonicalLinks();
		List<String> expected = Arrays.asList(
			"{\"sourceId\":\"id-3\",\"targetIds\":[\"clueweb12-0604wb-66-00231\"]}"
		);
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void approveURLSearchEncoding2() {
		Map<String, String> docIdToUrl = new HashMap<>();
		docIdToUrl.put("id-1", "http://google.com/");
		docIdToUrl.put("id-2", "http://google.de/");
		docIdToUrl.put("id-3", "http://en.wikipedia.org/wiki/Biloxi, Mississippi");
		JavaRDD<String> documentRepresentations = testDocumentsWithoutCanonicalLinks();
		List<String> expected = Arrays.asList(
			"{\"sourceId\":\"id-3\",\"targetIds\":[\"clueweb12-0604wb-66-00231\"]}"
		);
		
		List<String> actual = urlSearch(docIdToUrl, documentRepresentations);
		
		Assert.assertEquals(expected, actual);
	}
	
	private List<String> urlSearch(Map<String, String> docIdToUrl, JavaRDD<String> documentRepresentations) {
		JavaRDD<String> ret = JudgedDocumentsInDocumentRepresentations.urlSearch(docIdToUrl, documentRepresentations);
		
		return ret.collect();
	}

	private JavaRDD<String> emptyDocuments() {
		return jsc().parallelize(Arrays.asList());
	}
	
	private JavaRDD<String> testDocumentsWithoutCanonicalLinks() {
		return jsc().parallelize(Arrays.asList(
			"{\"docId\":\"clueweb12-0208wb-76-03608\",\"url\":\"http://www.mygift-store.com/verizon-phone-skin-decal-covers.html\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[-1623719936,30415,1245392,15079680],\"64BitK3SimHashThreeAndFiveGramms\":[-851640320,38096,12386317,15143424]},\"crawlingTimestamp\":\"2012-02-17T18:52:26Z\",\"documentLengthInWords\":355,\"documentLengthInWordsWithoutStopWords\":308}",
			"{\"docId\":\"clueweb12-0208wb-76-13634\",\"url\":\"http://www.mtwain.com/Roughing_It/60.html\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[-1196097536,19350,3604529,13424384],\"64BitK3SimHashThreeAndFiveGramms\":[1054932992,35638,15007919,11892992]},\"crawlingTimestamp\":\"2012-02-17T18:35:47Z\",\"documentLengthInWords\":2305,\"documentLengthInWordsWithoutStopWords\":1644}",
			"{\"docId\":\"clueweb12-0208wb-76-23649\",\"url\":\"http://www.merton.ox.ac.uk//fellows_and_research/chamberlain.shtml\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[-1839398912,22029,8126548,13582080],\"64BitK3SimHashThreeAndFiveGramms\":[-1009451008,38204,4784247,12509952]},\"crawlingTimestamp\":\"2012-02-17T19:31:18Z\",\"documentLengthInWords\":135,\"documentLengthInWordsWithoutStopWords\":108}",
			"{\"docId\":\"clueweb12-0208wb-76-33679\",\"url\":\"http://www.mcprod.com.br/geometry/_images/yampa-canyon-river-rafting/jugar-directamente-a-los-sims.html\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[-1421934592,5876,11403496,4329984],\"64BitK3SimHashThreeAndFiveGramms\":[-1729363968,42164,15532128,11390976]},\"crawlingTimestamp\":\"2012-02-17T19:31:40Z\",\"documentLengthInWords\":1868,\"documentLengthInWordsWithoutStopWords\":1350}",
			"{\"docId\":\"clueweb12-0604wb-66-00230\",\"url\":\"http://www.ukholidayaccommodation.net/lodges/county/Kent.html\",\"canonicalURL\":null,\"fingerprints\":{\"64BitK3SimHashOneGramms\":[1715601408,11878,9240695,5449216],\"64BitK3SimHashThreeAndFiveGramms\":[-1745289216,26434,4587628,3672320]},\"crawlingTimestamp\":\"2012-03-03T22:25:25Z\",\"documentLengthInWords\":1210,\"documentLengthInWordsWithoutStopWords\":890}",
			"{\"docId\":\"clueweb12-0604wb-66-00231\",\"url\":\"http://en.wikipedia.org/wiki/Biloxi,%20Mississippi\",\"canonicalURL\":null}"
		));
	}
}
