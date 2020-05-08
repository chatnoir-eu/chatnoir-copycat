package de.webis.cikm20_duplicates.spark.eval;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.approvaltests.Approvals;
import org.junit.Before;
import org.junit.Test;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.holdenkarau.spark.testing.SharedJavaSparkContext;

import de.webis.cikm20_duplicates.spark.SparkCreateSourceDocumentsIntegrationTest;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures.FeatureSetCandidate;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures.SimHashDocumentFeatures;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.SneakyThrows;

public class SparkEvaluateSimHashFeaturesIntegrationTest extends SharedJavaSparkContext {
	@Test
	public void calculateAllNGrammCombinations() {
		CollectionDocument doc = new CollectionDocument();
		doc.setFullyCanonicalizedContent("a b c d e f g h i j k l m n");
		
		Approvals.verifyAsJson(SparkEvaluateSimHashFeatures.allFeatures(doc));
	}
	
	private Fingerprinter<Integer> fingerprinter;
	
	@Before
	public void setUp() {
		int k = 3;
		int bitsInSimHash = 64;
		fingerprinter = FingerPrintUtil.simHashFingerPrinting(bitsInSimHash, k);
	}
	
	@Test
	public void transformExamplePairsIntoSimHashValues() {
		JavaPairRDD<String, SimHashDocumentFeatures> hashToFeatures = SparkEvaluateSimHashFeatures.featureHashToDocToFeatures(input(), fingerprinter);
		
		Approvals.verifyAsJson(tmp(hashToFeatures.map(i -> i)));
	}
	
	@Test
	public void transformExamplePairsIntoFeatureSetCandidates() {
		JavaRDD<FeatureSetCandidate> hashToFeatures = SparkEvaluateSimHashFeatures.featureSetCandidates(input(), fingerprinter);
		
		Approvals.verifyAsJson(tmp(hashToFeatures.map(i -> i)));
	}
	
	@Test
	public void groundTruthFeatureSetCandidates() {
		JavaRDD<FeatureSetCandidate> hashToFeatures = SparkEvaluateSimHashFeatures.groundTruth(input(), 0.6);
		
		Approvals.verifyAsJson(tmp(hashToFeatures.map(i -> i)));
	}
	
	@Test
	public void groundTruthFeatureSetCandidatesWithHigherThreshold() {
		JavaRDD<FeatureSetCandidate> hashToFeatures = SparkEvaluateSimHashFeatures.groundTruth(input(), 0.95);
		
		Approvals.verifyAsJson(tmp(hashToFeatures.map(i -> i)));
	}
	
	@Test
	public void reportGroundTruthBasicsForLowThreshold() {
		JavaRDD<String> hashToFeatures = SparkEvaluateSimHashFeatures.reportFeatureSetEvaluation(input(), fingerprinter, 0.6);
		List<String> actual = SparkCreateSourceDocumentsIntegrationTest.sorted(hashToFeatures);
		
		Approvals.verifyAsJson(actual);
	}
	
	@Test
	public void reportGroundTruthBasicsForHighThreshold() {
		JavaRDD<String> hashToFeatures = SparkEvaluateSimHashFeatures.reportFeatureSetEvaluation(input(), fingerprinter, 0.95);
		List<String> actual = SparkCreateSourceDocumentsIntegrationTest.sorted(hashToFeatures);
		
		Approvals.verifyAsJson(actual);
	}
	
	private static List<String> tmp(JavaRDD<?> t) {
		return SparkCreateSourceDocumentsIntegrationTest.sorted(t.map(i -> json(i)));
	}
	
	@SneakyThrows
	private static String json(Object o) {
		return new ObjectMapper().writeValueAsString(o);
	}
	
	private JavaRDD<String> input() {
		return jsc().parallelize(Arrays.asList(
				doc(1, "a b c d e f g h i j k l m n", 2, "a b c d e f g h i j k l m n", 1f),
				doc(1, "a b c d e f g h i j k l m n", 3, "a b c d e f g h i j k l m", .9f),
				doc(1, "a b c d e f g h i j k l m n", 2, "a b c d e f g h i j k l m n", 1f),
				doc(4, "o p q r s t u v w x y z u v", 5, "o p q r s u u v w x y z u v", .7f),
				doc(1, "a b c d e f g h i j k l m n", 2, "a b c d e f g h i j k l m n", 1f)
		));
	}
	
	private static String doc(int id, String text, int id2, String text2, float s3score) {
		return "{\"firstDoc\":{\"doc\": {\"id\": \""+ id +"\", \"fullyCanonicalizedContent\": \"" + text + "\"}},"
				+ "\"secondDoc\":{\"doc\": {\"id\": \"" + id2 + "\", \"fullyCanonicalizedContent\": \""+ text2 +"\"}},"
				+ "\"s3score\": " + s3score + "}";
	}
}
