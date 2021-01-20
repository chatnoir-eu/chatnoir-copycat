package de.webis.cikm20_duplicates.app;

import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.util.StringInputStream;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.cikm20_duplicates.spark.SparkEnrichRelevanceTransferPairs;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.DocumentResolver;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
public class DeduplicateTrecRunFile {
	
	private final int threads;
	
	private final DocumentResolver docResolver;
	
	private final SimilarityCalculation similarityCalculation;
	
	@SneakyThrows
	public List<String> deduplicate(String runFileContent) {
		Map<String, List<String>> topicToDocs = topicToSortedDocs(runFileContent);
		List<String> ret = new ArrayList<>();
		
		for(String topic: topicToDocs.keySet()) {
			System.out.println("Process topic " + topic);
			AllPairsSimilarities sim = deduplicateDocIds(topicToDocs.get(topic));
			sim.setTopic(topic);
			
			ret.add(sim.toString());
		}
		
		return ret;
	}
	
	private AllPairsSimilarities deduplicateDocIds(List<String> docs) {
		List<CollectionDocument> collectionDocs = parallel(() -> docs.parallelStream().map(id -> docResolver.loadCollectionDocument(id)).collect(Collectors.toList()));

		return deduplicateCollectionDocs(collectionDocs);
	}

	private AllPairsSimilarities deduplicateCollectionDocs(List<CollectionDocument> docs) {
		List<Pair<Integer, Integer>> pairsForComparison = pairsForComparison(docs);
		List<DocumentPairSimilarity> similarities = parallel(() -> pairsForComparison.parallelStream().map(p -> similarity(p, docs)).collect(Collectors.toList()));
		
		return new AllPairsSimilarities(null, similarities);
	}
	
	private List<Pair<Integer, Integer>> pairsForComparison(List<CollectionDocument> docs) {
		List<Pair<Integer, Integer>> ret = new ArrayList<>();
		
		for(int i=0; i<docs.size(); i++) {
			for(int j=i+1; j< docs.size(); j++) {
				ret.add(Pair.of(i, j));
			}
		}
		
		return ret;
	}
	
	private DocumentPairSimilarity similarity(Pair<Integer, Integer> idPair, List<CollectionDocument> docs) {
		DocumentPairSimilarity ret = new DocumentPairSimilarity();
		CollectionDocument first = docs.get(idPair.getLeft());
		CollectionDocument second = docs.get(idPair.getRight());
		failWhenDocsAreInWrongOrder(first, second);
		
		ret.setFirstId(first.getId());
		ret.setSecondId(second.getId());
		ret.setSimilarities(similarityCalculation.calculateSimilarities(first, second));
		
		return ret;
	}
	
	private void failWhenDocsAreInWrongOrder(CollectionDocument first, CollectionDocument second) {
		if(first == null || second == null || first.getId() == null || second.getId() == null || first.getId().compareTo(second.getId()) >= 0) {
			throw new RuntimeException("The pairs have a wrong order: '" + first.getId() +"' and '" + second.getId() + "'.");
		}
	}

	@SneakyThrows
	private Map<String, List<String>> topicToSortedDocs(String runFileContent) {
		return topicToSortedDocs(RunLine.parseRunlines(new StringInputStream(runFileContent)));
	}
	
	private Map<String, List<String>> topicToSortedDocs(List<RunLine> runLines) {
		Map<String, Set<String>> ret = new LinkedHashMap<>();
		
		for(RunLine runLine: runLines) {
			String topic = (runLine.getTopic() + "").trim();
			String doc = runLine.getDoucmentID().trim();
			
			if(!ret.containsKey(topic)) {
				ret.put(topic, new HashSet<>());
			}
			
			ret.get(topic).add(doc);
		}
		
		return sortValues(ret);
	}

	private Map<String, List<String>> sortValues(Map<String, Set<String>> map) {
		Map<String, List<String>> ret = new LinkedHashMap<>();
		
		for(String key: map.keySet()) {
			ret.put(key, sorted(map.get(key)));
		}
		
		return ret;
	}
	
	private List<String> sorted(Collection<String> c) {
		if(c == null) {
			return Collections.emptyList();
		}
		
		return new ArrayList<>(c.stream().sorted().collect(Collectors.toList()));
	}
	
	@SneakyThrows
	private <T> T parallel(Callable<T> callable) {
		ForkJoinPool pool = new ForkJoinPool(threads);
		return pool.submit(callable).get();
	}
	
	@Data
	@AllArgsConstructor
	private static class AllPairsSimilarities {
		private String topic;
		private List<DocumentPairSimilarity> similarities;
		
		@Override
		@SneakyThrows
		public String toString() {
			return new ObjectMapper().writeValueAsString(this);
		}
		
		@SneakyThrows
		public static AllPairsSimilarities fromString(String src) {
			return new ObjectMapper().readValue(src, AllPairsSimilarities.class);
		}
	}
	
	@Data
	@NoArgsConstructor
	private static class DocumentPairSimilarity {
		private String firstId;
		private String secondId;
		private Map<String, Float> similarities;
	}
	
	@Data
	public static class DefaultSimilarityCalculation implements SimilarityCalculation {
		private final static Map<String, BiFunction<CollectionDocument, CollectionDocument, Float>> PREDEFINED_SIMILARITIES = predefinedSimilarities();
		
		private final List<String> similarities;
		
		public DefaultSimilarityCalculation(List<String> similarities) {
			this.similarities = new ArrayList<>(similarities);
			failOnUnknownSimilarities();
		}
		
		private void failOnUnknownSimilarities() {
			if(similarities == null || similarities.size() == 0) {
				throw new RuntimeException("Please configure at least one similarity");
			}
			
			for(String sim: similarities) {
				if(!PREDEFINED_SIMILARITIES.containsKey(sim)) {
					throw new RuntimeException("Unknown similarity: " + sim);
				}
			}
		}

		private static Map<String, BiFunction<CollectionDocument, CollectionDocument, Float>> predefinedSimilarities() {
			Map<String, BiFunction<CollectionDocument, CollectionDocument, Float>> ret = new LinkedHashMap<>();
			ret.put("canonicalUrl", (a,b) -> canonicalUrlSimilarity(a,b));
			ret.put("s3Score", (a,b) -> (float) SparkEnrichRelevanceTransferPairs.s3Score(a, b));
			ret.put("cosineSimilarityThreeAndFiveGramms", (a,b) -> cosineSimilarityThreeAndFiveGramms(a,b));
			ret.put("cosineSimilarityEightGramms", (a,b) -> cosineSimilarityEightGramms(a,b));
			ret.put("cosineSimilarityOneGramms", (a,b) -> cosineSimilarityOneGramms(a,b));
			ret.put(oneGramFingerprinter().fingerprinterName(), (a,b) -> (float) oneGramFingerprinter().similarity(a, b));
			ret.put(threeAndFiveGramGramFingerprinter().fingerprinterName(), (a,b) -> (float) threeAndFiveGramGramFingerprinter().similarity(a, b));
			
			return ret;
		}
		
		private static Float cosineSimilarityOneGramms(CollectionDocument a, CollectionDocument b) {
			HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(a, 1), 64);
			HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(b, 1), 64);

			return (float) aVec.getCosSimilarity(bVec);
		}

		private static Float cosineSimilarityEightGramms(CollectionDocument a, CollectionDocument b) {
			HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(a, 8), 64);
			HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.nGramms(b, 8), 64);
			
			return (float) aVec.getCosSimilarity(bVec);
		}

		private static Float cosineSimilarityThreeAndFiveGramms(CollectionDocument a, CollectionDocument b) {
			HashVector aVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(a), 64);
			HashVector bVec = HashVectorSha3.toVector(SparkEvaluateSimHashFeatures.theeAndFiveGramms(b), 64);

			return (float) aVec.getCosSimilarity(bVec);
		}
		
		private static Float canonicalUrlSimilarity(CollectionDocument a, CollectionDocument b) {
			if(a == null) {
				return 0.0f;
			}
			
			String url = urlToString(a.getUrl());
			if(url != null && (url.equals(urlToString(b.getUrl())) || url.equals(urlToString(b.getCanonicalUrl())))) {
				return 1.0f;
			}
			
			url = urlToString(a.getCanonicalUrl());
			if(url != null && (url.equals(urlToString(b.getUrl())) || url.equals(urlToString(b.getCanonicalUrl())))) {
				return 1.0f;
			}
			
			return 0.0f;
		}
		
		private static String urlToString(URL url) {
			return url == null ? null : url.toString();
		}

		@Override
		public Map<String, Float> calculateSimilarities(CollectionDocument a, CollectionDocument b) {
			Map<String, Float> ret = new LinkedHashMap<>();
			
			for(String sim: similarities) {
				ret.put(sim, PREDEFINED_SIMILARITIES.get(sim).apply(a, b));
			}
			
			return ret;
		}
		
		private static Fingerprinter<Integer> oneGramFingerprinter() {
			return FingerPrintUtil.simHashFingerPrinting(64, 3);
		}
		
		private static Fingerprinter<Integer> threeAndFiveGramGramFingerprinter() {
			return FingerPrintUtil.productionFingerpringint(64, 3);
		}
	}
	
	public static interface SimilarityCalculation {
		Map<String, Float> calculateSimilarities(CollectionDocument a, CollectionDocument b);
	}
}
