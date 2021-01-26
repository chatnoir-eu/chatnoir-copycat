package de.webis.cikm20_duplicates.app;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import org.apache.commons.lang3.tuple.Pair;
import org.codehaus.jackson.map.ObjectMapper;

import com.amazonaws.util.StringInputStream;

import de.aitools.ir.fingerprinting.representation.HashVector;
import de.aitools.ir.fingerprinting.representation.HashVectorSha3;
import de.webis.cikm20_duplicates.spark.eval.SparkEvaluateSimHashFeatures;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil.DocumentResolver;
import de.webis.cikm20_duplicates.util.CollectionDocumentUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.trec_ndd.spark.DocumentHash;
import de.webis.trec_ndd.spark.RunLine;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3Score;
import de.webis.trec_ndd.spark.S3ScoreOnWord8GrammIndex.S3ScoreIntermediateResult;
import de.webis.trec_ndd.spark.SparkBuild8GrammIndex.Word8GrammIndexEntry;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import de.webis.trec_ndd.util.NGramms;
import de.webis.trec_ndd.util.NGramms.Word8Gramm;
import de.webis.trec_ndd.util.SymmetricPairUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

@Data
public class DeduplicateTrecRunFile {
	
	private final int threads;
	
	private final DocumentResolver docResolver;
	
	private final SimilarityCalculation similarityCalculation;
	
	private final double s3Threshold;
	
	private final int maxRank;
	
	private static final String ARG_DOC_RESOLVER = "documents";
	private static final String ARG_SIMILARITIES = "similarities";
	private static final String ARG_THREADS = "threads";
	private static final String ARG_S3_THRESHOLD = "s3Threshold";
	private static final String ARG_RANKS = "ranks";
	private static final String ARG_STRING_TRANSFORMATION = "stringTransformation";
	private static final String ARG_ANSERINI_INDEX = "anseriniIndex";
	
	@SneakyThrows
	public static void main(String[] args) {
		Namespace parsedArgs = parseArgs(args);
		if(parsedArgs == null) {
			return;
		}
		
		File outputFile = new File(parsedArgs.getString(ArgumentParsingUtil.ARG_OUTPUT));
		
		if(outputFile.exists()) {
			System.out.println("The specified " + ArgumentParsingUtil.ARG_OUTPUT + " '" + outputFile + "' exists.\nSkip...");
			return;
		}
		
		DocumentResolver docResolver = docResolver(parsedArgs);
		SimilarityCalculation sim = new DefaultSimilarityCalculation(parsedArgs.getList(ARG_SIMILARITIES));
		
		Path inputPath = Paths.get(parsedArgs.getString(ArgumentParsingUtil.ARG_INPUT));
		InputStream runFileContent = RunLine.openRunFile(inputPath);

		DeduplicateTrecRunFile dedup = new DeduplicateTrecRunFile(
			parsedArgs.getInt(ARG_THREADS),docResolver, sim, 
			parsedArgs.getDouble(ARG_S3_THRESHOLD), parsedArgs.getInt(ARG_RANKS)
		);
		try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputFile))) {
			dedup.deduplicate(runFileContent).forEach(i -> {
				try {
					writer.write(i +"\n");
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
		}
	}
	
	private static Namespace parseArgs(String[] args) {
		ArgumentParser parser = argParser();
		
		try {
			return parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			return null;
		}
	}

	private static ArgumentParser argParser() {
		ArgumentParser ret = ArgumentParsers.newFor("CopyCat: Deduplication of run files and qrels.")
				.build();
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_INPUT)
			.help("The run file or qrel file that should be deduplicated.")
			.required(true);
		
		ret.addArgument("--" + ArgumentParsingUtil.ARG_OUTPUT)
			.help("The result of the deduplication in jsonl format.")
			.required(true);
		
		ret.addArgument("--" + ARG_SIMILARITIES)
			.choices(DefaultSimilarityCalculation.PREDEFINED_SIMILARITIES.keySet())
			.help("Calculate all passed similarities.")
			.nargs("+");
		
		//FIXME: the documents are already transformed when they come from the index. But this is a very important point in the paper that we should bring: We support all and use the default string transformation.
		ret.addArgument("--" + ARG_STRING_TRANSFORMATION)
			.help("The anserini StringTransform that is used to transform the raw document into text. The default is JsoupStringTransform, which uses Jsoup to extract plain text out of HTML documents.")
			.setDefault("StringTransform");
		
		ret.addArgument("--" + ARG_DOC_RESOLVER)
			.choices("ChatNoirMapfiles", "AnseriniIndex")
			.help("Use the passed DocumentResolver to load the documents. E.g. AnseriniIndex loads documents by accessing a local anserini-index.")
			.required(true);
		
		ret.addArgument("--" + ARG_ANSERINI_INDEX)
			.help("When using AnseriniIndex as resolver for documents, we use the specified index.")
			.setDefault(".")
			.required(false);
	
		ret.addArgument("--" + ARG_RANKS)
			.help("Include documents up to the specified rank in the deduplication.")
			.type(Integer.class)
			.setDefault(1000)
			.required(false);
		
		ret.addArgument("--" + ARG_S3_THRESHOLD)
			.type(Double.class)
			.help("Report only near-duplicate pairs with s3 scores on word 8-grams above the specified threshold.")
			.setDefault(0.6);
		
		ret.addArgument("--" + ARG_THREADS)
			.type(Integer.class)
			.setDefault(1);

		return ret;
	}

	private static DocumentResolver docResolver(Namespace parsedArgs) {
		if(!"ChatNoirMapfiles".equals(parsedArgs.getString(ARG_DOC_RESOLVER))) {
			throw new RuntimeException("Unexpected " + ARG_DOC_RESOLVER + ": '" + parsedArgs.getString(ARG_DOC_RESOLVER) + "'.");
		}
		
		return CollectionDocumentUtil.HdfsMapFileDocumentResolver.smartDocumentResolver();
	}

	@SneakyThrows
	public Stream<String> deduplicate(String runFileContent) {
		return deduplicate(RunLine.parseRunlines(new StringInputStream(runFileContent)));
	}
	
	public Stream<String> deduplicate(InputStream runFileContent) {
		return deduplicate(RunLine.parseRunlines(runFileContent));
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
		List<SimilarityIntermediateProduct> s3Scores = intermediateResults.stream()
				.map(i -> new SimilarityIntermediateProduct(new S3Score(i), docs))
				.filter(i -> i.getS3Score().getS3Score() >= s3Threshold)
				.collect(Collectors.toList());
		
		List<DocumentPairSimilarity> similarities = parallel(() -> s3Scores.parallelStream().map(p -> similarity(p)).collect(Collectors.toList()));
		
		return new AllPairsSimilarities(null, similarities, idToHash.size());
	}
	
	@Data
	@AllArgsConstructor
	protected static class SimilarityIntermediateProduct {
		private final S3Score s3Score;
		private final CollectionDocument first, second;
		
		private SimilarityIntermediateProduct(S3Score s3Score, Map<String, CollectionDocument> docs) {
			this.s3Score = s3Score;
			this.first  = docs.get(s3Score.getIdPair().getLeft());
			this.second  = docs.get(s3Score.getIdPair().getRight());
		}
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
	
	private DocumentPairSimilarity similarity(SimilarityIntermediateProduct i) {
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
	private static class AllPairsSimilarities {
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
	private static class DocumentPairSimilarity {
		private String firstId;
		private String secondId;
		private Map<String, Float> similarities;
	}
	
	@Data
	public static class DefaultSimilarityCalculation implements SimilarityCalculation {
		private final static Map<String, Function<SimilarityIntermediateProduct, Float>> PREDEFINED_SIMILARITIES = predefinedSimilarities();
		
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

		private static Map<String, Function<SimilarityIntermediateProduct, Float>> predefinedSimilarities() {
			Map<String, Function<SimilarityIntermediateProduct, Float>> ret = new LinkedHashMap<>();
			ret.put("url", i -> canonicalUrlSimilarity(i.getFirst(), i.getSecond()));
			ret.put("s3", i -> (float) i.getS3Score().getS3Score());
			ret.put("cosine(3+5-grams)", i -> cosineSimilarityThreeAndFiveGramms(i.getFirst(), i.getSecond()));
			ret.put("cosine(8-grams)", i -> cosineSimilarityEightGramms(i.getFirst(), i.getSecond()));
			ret.put("cosine(1-grams)", i -> cosineSimilarityOneGramms(i.getFirst(), i.getSecond()));
			ret.put("simhash(1-grams)", i -> (float) oneGramFingerprinter().similarity(i.getFirst(), i.getSecond()));
			ret.put("simhash(3+5-grams)", i -> (float) threeAndFiveGramGramFingerprinter().similarity(i.getFirst(), i.getSecond()));
			
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
		public Map<String, Float> calculateSimilarities(SimilarityIntermediateProduct i) {
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
	
	public static interface SimilarityCalculation {
		Map<String, Float> calculateSimilarities(SimilarityIntermediateProduct i);
	}
}
