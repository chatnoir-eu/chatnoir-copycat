package de.webis.copycat_spark.app;

import java.io.InputStream;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ForkJoinPool;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.util.StringInputStream;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.copycat.DocumentPair;
import de.webis.copycat.DocumentResolver;
import de.webis.copycat.Similarities;
import de.webis.copycat_spark.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.copycat_spark.util.FingerPrintUtil;
import de.webis.copycat_spark.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.trec_collections.Qrel;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import de.webis.trec_ndd.util.SymmetricPairUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;

@Data
public class DeduplicateTrecRunFile {
	
	private final int threads;
	
	private final DocumentResolver docResolver;
	
	private final Similarities similarityCalculation;
	
	private final double s3Threshold;
	
	private final int maxRank;
	
	private final boolean deduplicateRunFile;
	
	private final boolean deduplicateQrelFile;

	@SneakyThrows
	public Stream<String> deduplicate(String fileContent) {
		return deduplicate(new StringInputStream(fileContent));
	}

	public Stream<String> deduplicate(InputStream fileContent) {
		failIfConfigurationIsInvalid();
		
		if(deduplicateRunFile) {
			return deduplicate(RunLine.parseRunlines(fileContent));
		} else {
			System.out.println("Deduplicate qrel file");
			return deduplicate(parseQrels(fileContent));
		}
	}
	
	@SneakyThrows
	private List<RunLine> parseQrels(InputStream stream) {
		return Collections.unmodifiableList(IOUtils.readLines(stream, StandardCharsets.UTF_8).stream()
			.filter(StringUtils::isNotEmpty)
			.map(Qrel::new)
			.map(i -> new RunLine(i.getTopicNumber() + " Q0 " + i.getDocumentID() + " 0"))
			.collect(Collectors.toList()));
	}

	private void failIfConfigurationIsInvalid() {
		if((deduplicateRunFile && deduplicateQrelFile) || (!deduplicateRunFile && !deduplicateQrelFile)) {
			throw new RuntimeException("Configuration is invalid. Exact one from deduplicateRunFile and deduplicateQrelFile must be true, but I got: " + deduplicateRunFile + " and " + deduplicateQrelFile);
		}
	}

	@SneakyThrows
	public Stream<String> deduplicate(List<RunLine> lines) {
		Map<String, List<String>> topicToDocs = topicToSortedDocs(lines);
		
		return topicToDocs.keySet().stream().map(i -> processTopic(i, topicToDocs.get(i)));
	}
	
	private String processTopic(String topic, List<String> docs) {
		System.out.println("Process topic " + topic);
		AllPairsSimilarities sim = deduplicateDocIds(docs);
		sim.setTopic(topic);
		
		return sim.toString();
	}
	
	private AllPairsSimilarities deduplicateDocIds(List<String> docs) {
		Map<String, CollectionDocument> collectionDocs = docs(docs);
		
		return deduplicateCollectionDocs(collectionDocs);
	}
	
	private Map<String, CollectionDocument> docs(List<String> docs) {
		List<CollectionDocument> collectionDocs = parallel(() -> docs.parallelStream().map(id -> docResolver.loadCollectionDocument(id)).collect(Collectors.toList()));
		Map<String, CollectionDocument> ret = new LinkedHashMap<>();
		
		for(CollectionDocument doc: collectionDocs) {
			if(doc != null && doc.getId() != null) {
				ret.put(doc.getId(), doc);
			}
		}
		
		return ret;
	}

	private AllPairsSimilarities deduplicateCollectionDocs(Map<String, CollectionDocument> docs) {
		Map<String, DocumentHash> idToHash = idToHash(docs);
		Map<Word8Gramm, List<String>> word8GrammToDocIds = word8GrammToDocIds(docs);
		word8GrammToDocIds = pruneEntries(word8GrammToDocIds);
		
		List<S3ScoreIntermediateResult> intermediateResults = sumCoocurrencesOfAllIndexEntries(word8GrammToDocIds, idToHash);
		List<DocumentPair> s3Scores = intermediateResults.stream()
				.map(i -> new DocumentPair(new S3Score(i), docs, idToHash))
				.filter(i -> i.getS3Score().getS3Score() >= s3Threshold)
				.collect(Collectors.toList());
		
		List<DocumentPairSimilarity> similarities = parallel(() -> s3Scores.parallelStream().map(p -> similarity(p)).collect(Collectors.toList()));
		
		return new AllPairsSimilarities(null, similarities, idToHash.size());
	}
	
	private List<S3ScoreIntermediateResult> sumCoocurrencesOfAllIndexEntries(Map<Word8Gramm, List<String>> word8GrammToDocIds, Map<String, DocumentHash> idToHash) {
		Map<Pair<String, String>, Integer> cooccurrences = new HashMap<>();
		
		for(Map.Entry<Word8Gramm, List<String>> e: word8GrammToDocIds.entrySet()) {
			Word8GrammIndexEntry indexEntry = new Word8GrammIndexEntry(e.getKey(), e.getValue());
			
			for(Pair<Pair<String, String>, Integer> pair: SymmetricPairUtil.extractCoocurrencePairs(indexEntry)) {
				int existingOccurences = cooccurrences.getOrDefault(pair.getLeft(), 0);
				
				cooccurrences.put(pair.getLeft(), existingOccurences + pair.getRight());
			}
		}
		
		List<S3ScoreIntermediateResult> ret = new ArrayList<>();
		
		for(Map.Entry<Pair<String, String>, Integer> e: cooccurrences.entrySet()) {
			S3ScoreIntermediateResult intermediateS3 = new S3ScoreIntermediateResult();
			intermediateS3.setCommonNGramms(e.getValue());
			intermediateS3.setIdPair(e.getKey());
			intermediateS3.setLeftMetadata(idToHash.get(e.getKey().getLeft()));
			intermediateS3.setRightMetadata(idToHash.get(e.getKey().getRight()));
			
			ret.add(intermediateS3);
		}
		
		return ret;
	}
	
	private Map<Word8Gramm, List<String>> pruneEntries(Map<Word8Gramm, List<String>> word8GrammToDocIds) {
		Map<Word8Gramm, List<String>> ret = new HashMap<>();
		
		for(Map.Entry<Word8Gramm, List<String>> entry: word8GrammToDocIds.entrySet()) {
			if(entry.getValue() != null && entry.getValue().size() > 1) {
				ret.put(entry.getKey(), entry.getValue());
			}
		}
		
		return ret;
	}

	private static Map<Word8Gramm, List<String>> word8GrammToDocIds(Map<String, CollectionDocument> docs) {
		Map<Word8Gramm, List<String>> ret = new HashMap<>();
		for(CollectionDocument doc: docs.values()) {
			for(Pair<Word8Gramm, String> word8Gramms: documentTo8Gramms(doc)) {
				if(!ret.containsKey(word8Gramms.getKey())) {
					ret.put(word8Gramms.getKey(), new ArrayList<>());
				}
				
				ret.get(word8Gramms.getKey()).add(word8Gramms.getValue());
			}
		}
		
		return ret;
	}
	
	private Map<String, DocumentHash> idToHash(Map<String, CollectionDocument> docs) {
		Map<String, DocumentHash> ret = new HashMap<>();
		
		for(CollectionDocument doc: docs.values()) {
			ret.put(doc.getId(), new DocumentHash(doc));
		}
		
		return ret;
	}

	//FIXME: Replace this with content from trec-ndd
	private static List<Pair<Word8Gramm, String>> documentTo8Gramms(CollectionDocument doc) {
		List<Pair<Word8Gramm, String>> ret = new LinkedList<>();
		String id = doc.getId();
		
		for(Word8Gramm nGramm :  NGramms.build8Gramms(doc.getFullyCanonicalizedContent())) {
			ret.add(Pair.of(nGramm, id));
		}
		
		return ret;
	}
	
	private DocumentPairSimilarity similarity(DocumentPair i) {
		DocumentPairSimilarity ret = new DocumentPairSimilarity();
		failWhenDocsAreInWrongOrder(i.getFirst(), i.getSecond());
		
		ret.setFirstId(i.getFirst().getId());
		ret.setSecondId(i.getSecond().getId());
		ret.setSimilarities(similarityCalculation.calculateSimilarities(i));
		
		return ret;
	}
	
	private void failWhenDocsAreInWrongOrder(CollectionDocument first, CollectionDocument second) {
		if(first == null || second == null || first.getId() == null || second.getId() == null || first.getId().compareTo(second.getId()) >= 0) {
			throw new RuntimeException("The pairs have a wrong order: '" + first.getId() +"' and '" + second.getId() + "'.");
		}
	}
	
	private Map<String, List<String>> topicToSortedDocs(List<RunLine> runLines) {
		Map<String, Set<String>> ret = new LinkedHashMap<>();
		runLines = runLines.stream().filter(i -> i.getRank() <= maxRank).collect(Collectors.toList());
		
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
	public static class AllPairsSimilarities {
		private String topic;
		private List<DocumentPairSimilarity> similarities;
		private int docs;
		
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
	public static class DocumentPairSimilarity {
		private String firstId;
		private String secondId;
		private Map<String, Float> similarities;
	}
	
	@Data
	public static class DefaultSimilarityCalculation implements Similarities {
		public static final Map<String, Function<DocumentPair, Float>> PREDEFINED_SIMILARITIES = predefinedSimilarities();
		
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

		private static Map<String, Function<DocumentPair, Float>> predefinedSimilarities() {
			Map<String, Function<DocumentPair, Float>> ret = new LinkedHashMap<>();
			ret.put("url", i -> canonicalUrlSimilarity(i.getFirst(), i.getSecond()));
			ret.put("s3", i -> (float) i.getS3Score().getS3Score());
			ret.put("cosine(3+5-grams)", i -> cosineSimilarityThreeAndFiveGramms(i.getFirst(), i.getSecond()));
			ret.put("cosine(8-grams)", i -> cosineSimilarityEightGramms(i.getFirst(), i.getSecond()));
			ret.put("cosine(1-grams)", i -> cosineSimilarityOneGramms(i.getFirst(), i.getSecond()));
			ret.put("simhash(1-grams)", i -> (float) oneGramFingerprinter().similarity(i.getFirst(), i.getSecond()));
			ret.put("simhash(3+5-grams)", i -> (float) threeAndFiveGramGramFingerprinter().similarity(i.getFirst(), i.getSecond()));
			ret.put("md5", i-> md5Similarity(i));
			ret.put("text-profile", i-> textProfileSimilarity(i));
			
			return ret;
		}
		
		private static Float textProfileSimilarity(DocumentPair i) {
			if(i.getFirstHash().getTextProfileSignature().equals(i.getSecondHash().getTextProfileSignature())) {
				return 1.0f;
			} else {
				return 0.0f;
			}
		}

		private static Float md5Similarity(DocumentPair i) {
			if(i.getFirstHash().getMd5().equals(i.getSecondHash().getMd5())) {
				return 1.0f;
			} else {
				return 0.0f;
			}
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
		public Map<String, Float> calculateSimilarities(DocumentPair i) {
			Map<String, Float> ret = new LinkedHashMap<>();
			
			for(String sim: similarities) {
				ret.put(sim, PREDEFINED_SIMILARITIES.get(sim).apply(i));
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
}
