package de.webis.cikm20_duplicates.spark.eval;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.spark.HashPartitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;

import de.aitools.ir.fingerprinting.representer.Hash;
import de.webis.cikm20_duplicates.spark.SparkCalculateCanonicalLinkGraphEdgeLabels.CanonicalLinkGraphEdge2;
import de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction.CanonicalLinkGraphEdge;
import de.webis.cikm20_duplicates.util.HashTransformationUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkEvaluateSimHashFeatures {

	private static final String DIR = "cikm2020/canonical-link-graph/";
	
	private static final String[] CORPORA = new String[] {/*"cw09", "cw12",*/ "cc-2015-11"};

//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus : CORPORA) {
//				JavaRDD<String> input = context.textFile(DIR + corpus + "-calulated-edges-sampled-large-groups");
//				
//				reportFeatureSetEvaluation(input, FingerPrintUtil.simHashFingerPrinting(64, 3), 0.8)
//					.saveAsTextFile(DIR + corpus + "-feature-set-evaluation");
//			}
//		}
//	}
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus : CORPORA) {
//				JavaRDD<FeatureSetCandidate> groundTruth = groundTruth(
//						context.textFile(DIR + corpus + "-calulated-edges-sampled-large-groups"),
//						0.8
//				);
//				JavaRDD<FeatureSetCandidate> candidates = featureSetCandidatesForCanonicalLinkGraphEdge(
//						context.textFile(DIR + corpus + "-sample-0.1-and-large-groups"),
//						FingerPrintUtil.simHashFingerPrinting(64, 3)
//				);
//				
//				reportFeatureSetEvaluation(candidates, groundTruth)
//					.saveAsTextFile(DIR + corpus + "-feature-set-evaluation");
//			}
//		}
//	}
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus : CORPORA) {
//				JavaRDD<String> input = context.textFile(DIR + corpus + "-sample-0.1-and-large-groups");
//				JavaPairRDD<String, DocToFeatures> docToFeatures = input.flatMap(i -> extractPairs(FingerPrintUtil.simHashFingerPrinting(64, 3), CanonicalLinkGraphEdge.fromString(i).getDoc()))
//						.mapToPair(i -> new Tuple2<>(i.docId, i));
//				
//				JavaPairRDD<String, SimHashDocumentFeatures> hashToDocFeatures = featureHashToDocToFeatures(docToFeatures);
//				
//				
//				hashToDocFeatures.map(i -> i._2().toString())
//					.saveAsTextFile(DIR + corpus + "-feature-set-sim-hash-document-features");
//			}
//		}
//	}
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus : CORPORA) {
				JavaRDD<SimHashDocumentFeatures> input = context.textFile(DIR + corpus + "-feature-set-sim-hash-document-features")
						.map(i -> SimHashDocumentFeatures.fromString(i));
				
				JavaPairRDD<String, SimHashDocumentFeatures> docFeatures = input.mapToPair(i -> new Tuple2<>(i.featureName + i.docId, i));
				input = docFeatures.groupByKey().map(i -> i._2.iterator().next());
				
				JavaPairRDD<String, SimHashDocumentFeatures> hashToDocFeatures = input.flatMapToPair(i -> extractAllFeatures(i));
				
//				JavaRDD<FeatureSetCandidate> candidates = hashToDocFeatures.groupByKey()
//						.flatMap(i -> reportFeatureSetCandidates(i, FingerPrintUtil.simHashFingerPrinting(64, 3)));
				
				hashToDocFeatures.repartitionAndSortWithinPartitions(new HashPartitioner(10000)).map(i -> BlaForTmp.persist(i))
					.saveAsTextFile(DIR + corpus + "-feature-set-hash-to-document-features");
			}
		}
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	public static class BlaForTmp {
		private String hash;
		private SimHashDocumentFeatures features;
		
		public static String persist(Tuple2<String, SimHashDocumentFeatures> i) {
			return new BlaForTmp(i._1(), i._2()).toString();
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}

		@SneakyThrows
		public static Tuple2<String, SimHashDocumentFeatures> fromString(String src) {
			BlaForTmp ret = new ObjectMapper().readValue(src, BlaForTmp.class);
			
			return new Tuple2<>(ret.hash, ret.features);
		}
	}
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus : CORPORA) {
//				JavaRDD<String> input = context.textFile(DIR + corpus + "-calulated-edges-sampled-large-groups");
//				JavaRDD<String> existingGroups = context.textFile(DIR + corpus + "-feature-set-evaluation");
//				
//				reportFeatureSetEvaluation(input, 0.8, existingGroups)
//					.saveAsTextFile(DIR + corpus + "-feature-set-evaluation-canonical-link-graph-edges");
//			}
//		}
//	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/evaluate-features");
	
		return new JavaSparkContext(conf);
	}
	
	public static Map<String, List<String>> allFeatures(CollectionDocument doc) {
		Map<String, List<String>> ret = new LinkedHashMap<>();
		
		List<String> oneGramms = nGramms(doc, 1);
		List<String> threeGramms = nGramms(doc, 3);
		List<String> fiveGramms = nGramms(doc, 5);
		List<String> eightGramms = nGramms(doc, 8);
		
		ret.put("1-gramms", oneGramms);
		ret.put("3-gramms", threeGramms);
		ret.put("5-gramms", fiveGramms);
		ret.put("8-gramms", eightGramms);

		ret.put("1-3-gramms", combine(oneGramms, threeGramms));
		ret.put("1-5-gramms", combine(oneGramms, fiveGramms));
		ret.put("1-8-gramms", combine(oneGramms, eightGramms));
		
		ret.put("3-5-gramms", combine(threeGramms, fiveGramms));
		ret.put("3-8-gramms", combine(threeGramms, eightGramms));
		
		ret.put("5-8-gramms", combine(fiveGramms, eightGramms));
		
		return ret;
	}
	
	private static List<String> nGramms(CollectionDocument doc, int length) {
		return Collections.unmodifiableList(new ArrayList<>(NGramms.nGramms(doc.getFullyCanonicalizedContent(), length)));
	}
	
	private static List<String> combine(List<String> a, List<String> b) {
		List<String> ret = new ArrayList<>(a);
		ret.addAll(b);
		
		return ret;
	}
	
	public static JavaRDD<FeatureSetCandidate> featureSetCandidatesForCanonicalLinkGraphEdge(JavaRDD<String> input, Fingerprinter<Integer> fingerprinter) {
		JavaPairRDD<String, DocToFeatures> docToFeatures = input.flatMap(i -> extractPairs(fingerprinter, CanonicalLinkGraphEdge.fromString(i).getDoc()))
				.mapToPair(i -> new Tuple2<>(i.docId, i));
		
		JavaPairRDD<String, SimHashDocumentFeatures> hashToDocFeatures = featureHashToDocToFeatures(docToFeatures);
		return hashToDocFeatures.groupByKey()
				.flatMap(i -> reportFeatureSetCandidates(i, fingerprinter));
	}
	
	public static JavaRDD<FeatureSetCandidate> featureSetCandidates(JavaRDD<String> input, Fingerprinter<Integer> fingerprinter) {
		JavaPairRDD<String, SimHashDocumentFeatures> hashToDocFeatures = featureHashToDocToFeatures(input, fingerprinter);
		
		return hashToDocFeatures.groupByKey()
				.flatMap(i -> reportFeatureSetCandidates(i, fingerprinter));
	}

	private static Iterator<FeatureSetCandidate> reportFeatureSetCandidates(Tuple2<String, Iterable<SimHashDocumentFeatures>> a, Fingerprinter<Integer> fingerprinter) {
		List<SimHashDocumentFeatures> docs = new ArrayList<>(ImmutableList.copyOf(a._2.iterator()));
		List<FeatureSetCandidate> ret = new ArrayList<>();
		
		for(int i=0; i< docs.size(); i++) {
			SimHashDocumentFeatures aFeatures = docs.get(i);
			
			byte[] hashA = HashTransformationUtil.integersToHash(aFeatures.simHash);
			for(int j=i+1; j<docs.size(); j++) {
				SimHashDocumentFeatures bFeatures = docs.get(j);
				
				if(!aFeatures.featureName.equals(bFeatures.featureName)) {
					continue;
				}
				
				byte[] hashB = HashTransformationUtil.integersToHash(bFeatures.simHash);
				
				int hemming = Hash.getHammingDistance(hashA, hashB);
				if(hemming <= 3) {
					ret.add(FeatureSetCandidate.featureSetCandidateOrNull(aFeatures, bFeatures));
				}
			}
		}
		
		return ret.stream().filter(i -> i != null).iterator();
	}


	public static JavaRDD<String> reportFeatureSetEvaluation(JavaRDD<String> input, double threshold, JavaRDD<String> existingGroups) {
		JavaRDD<FeatureSetCandidate> b = groundTruth(input, threshold);
		JavaPairRDD<Tuple2<String, String>, String> t = b.mapToPair(i -> new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>(i.firstId, i.secondId), i.featureName));
		
		JavaPairRDD<Tuple2<String, String>, String> ret = existingGroups.mapToPair(i -> tmp(i));

		return ret.join(t).flatMapToPair(i -> reportSecondEvaluationForFeatureSet(i))
				.groupByKey()
				.map(i -> reportEvaluationForFeatureSet(i));
	}

	@SneakyThrows
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static Iterator<Tuple2<Tuple2<String, String> ,String>> reportSecondEvaluationForFeatureSet(Tuple2<Tuple2<String, String>, Tuple2<String, String>> orig) {
		List<String> features = new ArrayList<>(Arrays.<String>asList(orig._2()._2()));
		Map<String, Object> tmp2 = new ObjectMapper().readValue(orig._2()._1(), Map.class);
		features.addAll((List) tmp2.get("featureNames"));
		
		Stream<Tuple2<Tuple2<String, String> ,String>> ret = features.stream().collect(Collectors.toSet()).stream().map(i -> new Tuple2<>(orig._1(), i));
		return ret.iterator();
	}

	public static JavaRDD<String> reportFeatureSetEvaluation(JavaRDD<String> input, Fingerprinter<Integer> fingerprinter, double threshold) {
		return reportFeatureSetEvaluation(featureSetCandidates(input, fingerprinter), groundTruth(input, threshold));
	}
	
	public static JavaRDD<String> reportFeatureSetEvaluation(JavaRDD<FeatureSetCandidate> candidates, JavaRDD<FeatureSetCandidate> groundTruth) {
		JavaPairRDD<Tuple2<String, String> ,String> ret = candidates.union(groundTruth)
				.mapToPair(i -> new Tuple2<>(new Tuple2<>(i.firstId, i.secondId), i.featureName));
		
		return ret.groupByKey()
				.map(i -> reportEvaluationForFeatureSet(i));
	}
	
	@SneakyThrows
	private static String reportEvaluationForFeatureSet(Tuple2<Tuple2<String, String>, Iterable<String>> i) {
		Set<String> features = new HashSet<>(); 
		i._2.iterator().forEachRemaining(b -> features.add(b));
		List<String> tmp = new LinkedList<>(features);
		Collections.sort(tmp);
		
		Map<String, Object> ret = new LinkedHashMap<>();
		ret.put("firstId", i._1._1);
		ret.put("secondId", i._1._2);
		ret.put("featureNames", tmp);
		
		return new ObjectMapper().writeValueAsString(ret);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static Tuple2<Tuple2<String, String>, String> tmp(String src) {
		Map<String, Object> ret = new ObjectMapper().readValue(src, Map.class);
		
		return new Tuple2<Tuple2<String, String>, String>(new Tuple2<String, String>((String)ret.get("firstId"), (String)ret.get("secondId")), src);
	}

	public static JavaRDD<FeatureSetCandidate> groundTruth(JavaRDD<String> input, double threshold) {
		return input.map(i -> groundTruthOrNull(i, threshold)).filter(i -> i != null);
	}
	
	public static JavaPairRDD<String, SimHashDocumentFeatures> featureHashToDocToFeatures(JavaRDD<String> input, Fingerprinter<Integer> fingerprinter) {
		JavaPairRDD<String, DocToFeatures> docToFeatures = input.flatMap(i -> extractPairs(i, fingerprinter))
				.mapToPair(i -> new Tuple2<>(i.docId, i));
		
		return featureHashToDocToFeatures(docToFeatures);
	}

	public static JavaPairRDD<String, SimHashDocumentFeatures> featureHashToDocToFeatures(JavaPairRDD<String, DocToFeatures> docToFeatures) {
		docToFeatures = docToFeatures.groupByKey()
			.mapToPair(i -> keepOnlyFirst(i));
		
		return docToFeatures.flatMapToPair(i -> extractAllFeatures(i._2())).filter(i -> i != null);
	}
	
	private static Iterator<Tuple2<String, SimHashDocumentFeatures>> extractAllFeatures(DocToFeatures i) {
		List<Tuple2<String, SimHashDocumentFeatures>> ret = new ArrayList<>();
		
		for(SimHashDocumentFeatures featureSet: i.features) {
			for(Integer feature: featureSet.simHash) {
				String key = featureSet.featureName + "___" + feature;
				ret.add(new Tuple2<>(key, featureSet));
			}
		}
		
		return ret.iterator();
	}
	
	private static Iterator<Tuple2<String, SimHashDocumentFeatures>> extractAllFeatures(SimHashDocumentFeatures i) {
		List<Tuple2<String, SimHashDocumentFeatures>> ret = new ArrayList<>();
		
		for(Integer feature: i.simHash) {
			String key = i.featureName + "___" + feature;
			ret.add(new Tuple2<>(key, i));
		}
		
		return ret.iterator();
	}

	private static Tuple2<String, DocToFeatures> keepOnlyFirst(Tuple2<String, Iterable<DocToFeatures>> i) {
		return new Tuple2<>(i._1(), i._2().iterator().next());
	}

	private static Iterator<DocToFeatures> extractPairs(String src, Fingerprinter<Integer> fingerprinter) {
		CanonicalLinkGraphEdge2 edge = CanonicalLinkGraphEdge2.fromString(src);
		
		return extractPairs(fingerprinter, edge.getFirstDoc().getDoc(), edge.getSecondDoc().getDoc());
	}
	
	private static Iterator<DocToFeatures> extractPairs(Fingerprinter<Integer> fingerprinter, CollectionDocument...docs) {
		return Arrays.asList(docs).stream()
				.map(i -> new DocToFeatures(i, fingerprinter))
				.iterator();
	}
	
	private static FeatureSetCandidate groundTruthOrNull(String src, double threshold) {
		CanonicalLinkGraphEdge2 edge = CanonicalLinkGraphEdge2.fromString(src);
		CollectionDocument a = edge.getFirstDoc().getDoc();
		CollectionDocument b = edge.getSecondDoc().getDoc();
		String label = "S3";
		
		if(edge.getS3score() < threshold) {
			label = "S3-negative";
		}
		
		if(a.getId().compareTo(b.getId()) < 0) {
			return new FeatureSetCandidate(label, a.getId(), b.getId());
		} else if (a.getId().compareTo(b.getId()) > 0) {
			return new FeatureSetCandidate(label, b.getId(), a.getId());
		} else {
			return null;
		}
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class SimHashDocumentFeatures implements Serializable {
		private String featureName;
		private String docId;
		private List<Integer> simHash;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static SimHashDocumentFeatures fromString(String src) {
			return new ObjectMapper().readValue(src, SimHashDocumentFeatures.class);
		}
	}
	
	@Data
	@SuppressWarnings("serial")
	public static class DocToFeatures implements Serializable {
		private final String docId;
		private final List<SimHashDocumentFeatures> features;
		
		public DocToFeatures(CollectionDocument doc, Fingerprinter<Integer> fingerprinter) {
			this.docId = doc.getId();
			Map<String, List<String>> allFeatures = allFeatures(doc);
			features = new ArrayList<>();
			
			for(String featureName: allFeatures.keySet()) {
				features.add(new SimHashDocumentFeatures(featureName, doc.getId(), fingerprinter.fingerprint(allFeatures.get(featureName))));
			}
		}
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class FeatureSetCandidate implements Serializable {
		private String featureName;
		private String firstId;
		private String secondId;
		
		public static FeatureSetCandidate featureSetCandidateOrNull(SimHashDocumentFeatures a, SimHashDocumentFeatures b) {
			if(a == null || a.docId == null || b == null || b.docId == null || a.docId.compareTo(b.docId) == 0) {
				return null;
			}
			
			if(a.docId.compareTo(b.docId) < 0) {
				return new FeatureSetCandidate(a.featureName, a.docId, b.docId);
			} else if (a.docId.compareTo(b.docId) > 0) {
				return new FeatureSetCandidate(a.featureName, b.docId, a.docId);
			} else {
				return null;
			}
		}
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static FeatureSetCandidate fromString(String src) {
			return new ObjectMapper().readValue(src, FeatureSetCandidate.class);
		}
	}
}
