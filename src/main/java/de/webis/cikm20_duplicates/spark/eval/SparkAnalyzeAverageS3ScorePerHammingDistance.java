package de.webis.cikm20_duplicates.spark.eval;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.cikm20_duplicates.spark.SparkCalculateCanonicalLinkGraphEdgeLabels.CanonicalLinkGraphEdge2;
import de.webis.cikm20_duplicates.spark.eval.SparkCalculatePrecisionInCanonicalLinkGraph.TwoDocsForFeatureWithS3Score;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.HashTransformationUtil;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import lombok.SneakyThrows;

public class SparkAnalyzeAverageS3ScorePerHammingDistance {
	
	private static final String[] CORPORA = new String[] {"cw09", "cw12", "cc-2015-11", "cc-2017-04"};
	
//	public static void main(String[] args) {
//		String corpus = CORPORA[0];
//		
//		try(JavaSparkContext jsc = context()) {
//			for(String feature: featureNames()) {
//				Fingerprinter<Integer> f = fingerprinter(feature);
//				JavaRDD<TwoDocsForFeatureWithS3Score> input = jsc.textFile(input(corpus, feature))
//						.map(src -> TwoDocsForFeatureWithS3Score.fromString(src));
//				
//				input.map(i -> calculateHammingDistance(f, i))
//					.saveAsTextFile("cikm2020/canonical-link-graph/production-tests/" + corpus + "-" + feature + ".jsonl");
//			}
//		}
//	}
	
	public static void main(String[] args) {
	
		try(JavaSparkContext jsc = context()) {
			for(String corpus: CORPORA) {
				Fingerprinter<Integer> f = fingerprinter("1-gramms");
				JavaRDD<CanonicalLinkGraphEdge2> input = jsc.textFile("cikm2020/canonical-link-graph/" + corpus + "-calulated-edges-sampled-large-groups")
						.map(src -> CanonicalLinkGraphEdge2.fromString(src));
				
				input.map(i -> calculateHammingDistance(f, i))
					.saveAsTextFile("cikm2020/canonical-link-graph/production-tests/" + corpus + "-canonical-link-edges-1-gramms.jsonl");
			}
		}
	}


	private static Object calculateHammingDistance(Fingerprinter<Integer> f, CanonicalLinkGraphEdge2 i) {
		return calculateHammingDistance(f, i.getFirstDoc().getDoc(), i.getSecondDoc().getDoc(), i.getS3score());
	}
	
	@SneakyThrows
	private static String calculateHammingDistance(Fingerprinter<Integer> fingerprinter, TwoDocsForFeatureWithS3Score i) {
		return calculateHammingDistance(fingerprinter, i.getLeftDoc(), i.getRightDoc(), i.getS3Score());
	}
	
	@SneakyThrows
	private static String calculateHammingDistance(Fingerprinter<Integer> fingerprinter, CollectionDocument a, CollectionDocument b, double s3Score) {
		List<Integer> left = fingerprinter.fingerprint(a);
		List<Integer> right = fingerprinter.fingerprint(b);
		byte[] leftArray = HashTransformationUtil.integersToHash(left);
		byte[] rightArray = HashTransformationUtil.integersToHash(right);
		
		Map<String, Object> ret = new HashMap<>();
		ret.put("hammingDistance", Hash.getHammingDistance(leftArray, rightArray));
		ret.put("s3Score", s3Score);
		
		return new ObjectMapper().writeValueAsString(ret);
	}

	private static Fingerprinter<Integer> fingerprinter(String featureName) {
		if("1-gramms".equals(featureName)) {
			return FingerPrintUtil.simHashFingerPrinting(64, 3);
		} else if("3-5-gramms".equals(featureName)) {
			return FingerPrintUtil.productionFingerpringint(64, 3);	
		}
		
		throw new RuntimeException("Can not handle: " + featureName);
	}

	private static List<String> featureNames() {
		return Arrays.asList("1-gramms", "3-5-gramms");
	}
	
	private static String input(String corpus, String features) {
		return "cikm2020/canonical-link-graph/feature-set-precision-experiments/" + corpus + "-" + features + "-raw-data.jsonl";
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/s3-scores-from-sample");
		
		return new JavaSparkContext(conf);
	}
}
