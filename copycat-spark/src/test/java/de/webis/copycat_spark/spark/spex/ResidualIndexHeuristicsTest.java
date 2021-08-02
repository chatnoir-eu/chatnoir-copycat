package de.webis.copycat_spark.spark.spex;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.spark.api.java.JavaPairRDD;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Test;

import de.webis.copycat_spark.spark.SparkIntegrationTestBase;
import de.webis.copycat_spark.spark.spex.ResidualIndex.ResidualIndexEntry;
import de.webis.copycat_spark.spark.spex.ResidualIndexHeuristics.ResidualIndexHeuristic;
import de.webis.trec_ndd.spark.DocumentHash;
import lombok.SneakyThrows;

public class ResidualIndexHeuristicsTest  extends SparkIntegrationTestBase {
	
	@Test
	public void testResidualHeuristicsSortedAboveOne() {
		List<ResidualIndexHeuristic> expected = Arrays.asList(new ResidualIndexHeuristic("id-3", 26, 26));
		float threshold = 1f;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-3", 26), doc("id-1", 48), doc("id-11", 1611));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-1", 18), residualEntry("id-3", 26), residualEntry("id-11", 36));
		List<ResidualIndexHeuristic> actual = sortedHeuristics(documentMetadata, residualIndex, threshold);

		Assert.assertEquals(expected, actual);
	}
	
	@Test
	public void testResidualHeuristicsSortedAboveZero() {
		String expected = Arrays.asList(new ResidualIndexHeuristic("id-3", 26, 26), new ResidualIndexHeuristic("id-1", 18, 48), new ResidualIndexHeuristic("id-11", 36, 1611)).toString();
		float threshold = 0f;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-3", 26), doc("id-1", 48), doc("id-11", 1611));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-1", 18), residualEntry("id-3", 26), residualEntry("id-11", 36));
		String actual = sortedHeuristics(documentMetadata, residualIndex, threshold).toString();

		Assert.assertEquals(expected.toString(), actual);
	}
	
	@Test
	public void testResidualHeuristicAbovePointFive() {
		ResidualIndexHeuristic expected = new ResidualIndexHeuristic("id-1", 18, 48);
		float expectedS3Bound = 0.75f;
		float threshold = 0.5f;
		int expectedOtherDocRange = 0;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-1", 48));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-1", 18));
		ResidualIndexHeuristic actual = heuristic(documentMetadata, residualIndex);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedS3Bound, actual.getUpperS3Bound(), 0.0001);
		Assert.assertEquals(expectedOtherDocRange, actual.getOtherDocRange(threshold));
	}
	
	@Test
	public void testResidualHeuristicAboveTwo() {
		ResidualIndexHeuristic expected = new ResidualIndexHeuristic("id-3", 26, 26);
		float expectedS3Bound = 2f;
		float threshold = 0.5f;
		int expectedOtherDocRange = 53;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-3", 26));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-3", 26));
		ResidualIndexHeuristic actual = heuristic(documentMetadata, residualIndex);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedS3Bound, actual.getUpperS3Bound(), 0.0001);
		Assert.assertEquals(expectedOtherDocRange, actual.getOtherDocRange(threshold));
	}
	
	@Test
	public void testResidualHeuristicAboveTwoAndOtherDocRange3() {
		ResidualIndexHeuristic expected = new ResidualIndexHeuristic("id-a", 1, 1);
		float expectedS3Bound = 2f;
		float threshold = 0.5f;
		int expectedOtherDocRange = 3;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-a", 1));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-a", 1));
		ResidualIndexHeuristic actual = heuristic(documentMetadata, residualIndex);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedS3Bound, actual.getUpperS3Bound(), 0.0001);
		Assert.assertEquals(expectedOtherDocRange, actual.getOtherDocRange(threshold));
	}
	
	@Test
	public void testResidualHeuristicAbovePointFiveAndOtherDocRange0() {
		ResidualIndexHeuristic expected = new ResidualIndexHeuristic("id-a", 4608, 16775);
		float expectedS3Bound = 0.549389f;
		float threshold = 0.5f;
		int expectedOtherDocRange = 0;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-a", 16775));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-a", 4608));
		ResidualIndexHeuristic actual = heuristic(documentMetadata, residualIndex);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedS3Bound, actual.getUpperS3Bound(), 0.0001);
		Assert.assertEquals(expectedOtherDocRange, actual.getOtherDocRange(threshold));
	}
	
	@Test
	public void testResidualHeuristicNearZero() {
		ResidualIndexHeuristic expected = new ResidualIndexHeuristic("id-11", 36, 1611);
		float expectedS3Bound = 0.044693f;
		float threshold = 0.5f;
		int expectedOtherDocRange = 0;
		
		JavaPairRDD<String, DocumentHash> documentMetadata = documentMetadata(doc("id-11", 1611));
		JavaPairRDD<String, ResidualIndexEntry> residualIndex = residualIndex(residualEntry("id-11", 36));
		ResidualIndexHeuristic actual = heuristic(documentMetadata, residualIndex);
		
		Assert.assertEquals(expected, actual);
		Assert.assertEquals(expectedS3Bound, actual.getUpperS3Bound(), 0.0001);
		Assert.assertEquals(expectedOtherDocRange, actual.getOtherDocRange(threshold));
	}
	
	private JavaPairRDD<String, DocumentHash> documentMetadata(String...documentHashes) {
		return FullS3Deduplication.documentMetadata(jsc().parallelize(Arrays.asList(documentHashes)));
	}
	
	private JavaPairRDD<String, ResidualIndexEntry> residualIndex(String...indexEntry) {
		return FullS3Deduplication.residualIndexEntry(jsc().parallelize(Arrays.asList(indexEntry)));
	}
	
	@SneakyThrows
	private static String residualEntry(String id, int residualTokens) {
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("documentId", id);
		ret.put("residualTokens", IntStream.range(0, residualTokens).mapToObj(i -> String.valueOf(i)).collect(Collectors.toList()));
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	private static String doc(String id, int length) {
		return "{\"id\":\"" + id + "\",\"textProfileSignature\":\"a\",\"md5\":\"a\",\"fullyTextProfileSignature\":\"a\",\"fullyCanonicalizedMd5\":\"a\",\"documentLength\":0,\"fullyCanonicalizedWord8GrammCount\":0,\"fullyCanonicalizedWord8GrammSetSize\":" + length + "}";
	}
	
	private List<ResidualIndexHeuristic> sortedHeuristics(JavaPairRDD<String, DocumentHash> metadata, JavaPairRDD<String, ResidualIndexEntry> residualIndex, double threshold) {
		return ResidualIndexHeuristics.sortedHeuristics(metadata, residualIndex, threshold);
	}
	
	private ResidualIndexHeuristic heuristic(JavaPairRDD<String, DocumentHash> metadata, JavaPairRDD<String, ResidualIndexEntry> residualIndex) {
		List<ResidualIndexHeuristic> ret = heuristics(metadata, residualIndex);
		if(ret.size() != 1) {
			throw new RuntimeException("");
		}
		
		return ret.get(0);
	}
	
	private List<ResidualIndexHeuristic> heuristics(JavaPairRDD<String, DocumentHash> metadata, JavaPairRDD<String, ResidualIndexEntry> residualIndex) {
		return ResidualIndexHeuristics.heuristics(metadata, residualIndex).collect();
	}
}
