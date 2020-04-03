package de.webis.cikm20_duplicates.spark;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.curator.shaded.com.google.common.collect.Iterators;
import org.apache.htrace.shaded.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.cikm20_duplicates.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.Data;
import lombok.SneakyThrows;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkCreateDeduplicationCandidates {

	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("cikm2020/document-fingerprints");
			
//			JavaRDD<String> duplicationCandidates = duplicationCandidatesFromFingerprints(input)
//				.map(i -> i.toString());
//			
			duplicationCandidatePairsFromFingerprints(input, DeduplicationStrategy.simHashDeduplication(10000))
				.saveAsTextFile("cikm2020/candidate-pairs");
			
//			hashPartitionToDocument(input, DeduplicationStrategy.simHashDeduplication(6000))
//				.saveAsTextFile("cikm2020/input-with-new-partition");
			
//			hashPartitionToDocument(input, DeduplicationStrategy.SIM_HASH_DEDUPLICATION_STRATEGY)
//				.repartition(6000)
//				.aggregateByKey(0l, (count, doc) -> Long.valueOf((long)(count +1)), (i,j) -> Long.valueOf((long) i+j))
//				.map(i -> "{\"hash-partition\": "+ i._1() +", \"count\": "+ i._2() +"}")
//				.saveAsTextFile("cikm2020/count-per-sim-hash-partition");
		}
	}
	
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/candidates");

		return new JavaSparkContext(conf);
	}
	
	public static JavaPairRDD<String, Iterable<Tuple2<String, CollectionDocument>>> topicsToImportantDocuments(JavaRDD<String> rdd) {
		JavaRDD<Tuple2<String, CollectionDocument>> ret = rdd
				.map(i -> CollectionDocumentWithTopics.fromString(i))
				.flatMap(i -> flattenTopicsForDoc(i));
 
		return ret.groupBy(i -> i._1());
	}

	private static Iterator<Tuple2<String, CollectionDocument>> flattenTopicsForDoc(CollectionDocumentWithTopics docWithTopics) {
		return docWithTopics.getTopics().stream()
				.map(topic -> new Tuple2<>(topic, docWithTopics.getDoc()))
				.iterator();
	}

	public static <T extends Comparable<T>> JavaRDD<Tuple2<String, Set<T>>> topicsToFingerPrintsOfImportantDocsPerTopic(JavaRDD<String> rdd, Fingerprinter<T> fingerprinter) {
		return topicsToImportantDocuments(rdd)
				.map(i -> new Tuple2<>(i._1, combineAll(i._2, fingerprinter)));
	}

	private static <T extends Comparable<T>> Set<T> combineAll(Iterable<Tuple2<String, CollectionDocument>> a, Fingerprinter<T> fingerprinter) {
		Set<T> ret = new HashSet<>();
		Iterator<CollectionDocument> docIterator = Iterators.transform(a.iterator(), i -> i._2());
		docIterator.forEachRemaining(doc -> ret.addAll(fingerprinter.fingerprint(doc)));

		return ret;
	}

	public static JavaRDD<Tuple2<String, CollectionDocument>> candidatesForAllSourceDocuments(JavaSparkContext context, JavaRDD<String> sourceDocuments, Fingerprinter<Integer> fingerprinter, AnseriniCollectionReader<Document> acr) {
		Map<String, BloomFilter<Integer>> topicBloomFilters = new HashMap<>();
		
		Map<String, Set<Integer>> topicToFingerPrintUnits = topicsToFingerPrintsOfImportantDocsPerTopic(sourceDocuments, fingerprinter)
				.collect().stream()
				.collect(Collectors.toMap(i -> i._1(), i -> i._2()));
		int overallElements = topicToFingerPrintUnits.values().stream().mapToInt(i -> i.size()).sum();
		
		BloomFilter<Integer> bf = BloomFilter.create(Funnels.integerFunnel(), overallElements + 100000, 1.0e-8);

		for(Entry<String, Set<Integer>> topicToFingerpintUnit : topicToFingerPrintUnits.entrySet()) {
			BloomFilter<Integer> topicBf = BloomFilter.create(Funnels.integerFunnel(), topicToFingerpintUnit.getValue().size() + 10000, 1.0e-8);
			
			for(Integer fingerprintUnit: topicToFingerpintUnit.getValue()) {
				topicBf.put(fingerprintUnit);
				bf.put(fingerprintUnit);
			}
			
			topicBloomFilters.put(topicToFingerpintUnit.getKey(), topicBf);
		}
		
		JavaRDD<CollectionDocument> allDocs = context.parallelize(acr.segmentPaths())
			.flatMap(s -> acr.collectionDocumentsInPath(s));
		
		return allDocs.flatMap(doc -> docToCandidateTopics(fingerprinter, doc, bf, topicBloomFilters));
	}
	
	private static <T extends Comparable<T>> Iterator<Tuple2<String, CollectionDocument>> docToCandidateTopics(Fingerprinter<T> fingerprinter, CollectionDocument doc, BloomFilter<T> bf, Map<String, BloomFilter<T>> topicBloomFilters) {
		List<T> fingerPrint = fingerprinter.fingerprint(doc);
		Set<String> candidateTopics = new HashSet<>();
		
		for(T fingerPrintUnit: fingerPrint) {
			if(bf.mightContain(fingerPrintUnit)) {
				for(Entry<String, BloomFilter<T>> e : topicBloomFilters.entrySet()) {
					if(e.getValue().mightContain(fingerPrintUnit)) {
						candidateTopics.add(e.getKey());
					}
				}
			}
		}
		
		return candidateTopics.stream()
				.map(i -> new Tuple2<>(i, doc))
				.iterator();
	}

	private static JavaPairRDD<Integer, DeduplicationUnit> hashPartitionToDocument(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		return docsWithFingerprint
				.map(i -> DocumentWithFingerprint.fromString(i))
				.flatMapToPair(doc -> extractHashesToDocId(doc, f))
				.repartitionAndSortWithinPartitions(f.getPartitioner());
	}
	
	public static JavaRDD<String> duplicationCandidatePairsFromFingerprints(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		JavaPairRDD<Integer, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		return parsedInput.join(parsedInput)
				.map(i -> emitPairOrNull(i._2()))
				.filter(i -> i != null)
				.distinct();
	}
	
	public static List<Integer> useMinHash(DocumentWithFingerprint doc) {
		return doc.getMinHashParts();
	}
	
	public static List<Integer> useSimHash(DocumentWithFingerprint doc) {
		return doc.getSimHash65BitParts();
	}
	
	@SneakyThrows
	private static String emitPairOrNull(Tuple2<DeduplicationUnit, DeduplicationUnit> pair) {
		if(pair == null || pair._1() == null || pair._2() == null || pair._1().getId().compareTo(pair._2().getId()) >= 0) {
			return null;
		}

		Map<String, Object> candidate = new LinkedHashMap<>();
		candidate.put("firstId", pair._1().getId());
		candidate.put("secondId", pair._2().getId());
		candidate.put("firstFingerprintComponents", pair._1().getHashParts());
		candidate.put("secondFingerprintComponents", pair._2().getHashParts());
				
		return new ObjectMapper().writeValueAsString(candidate);
	}

	private static Iterator<Tuple2<Integer, DeduplicationUnit>> extractHashesToDocId(DocumentWithFingerprint doc, DeduplicationStrategy f) {
		List<Integer> hashParts = f.extract(doc);
		
		return hashParts.stream()
				.map(hash -> new Tuple2<>(hash, new DeduplicationUnit(doc.getDocId(), hashParts)))
				.iterator();
	}
	
	public static JavaRDD<DocumentWithFingerprint> duplicationCandidatesFromFingerprints(JavaRDD<String> docsWithFingerprint) {
		JavaRDD<DocumentWithFingerprint> parsedInput = docsWithFingerprint.map(i -> DocumentWithFingerprint.fromString(i));
		BloomFilter<Integer> bf = bf(parsedInput);
		
		return parsedInput.filter(doc -> doc.getMinHashParts().stream().anyMatch(i -> bf.mightContain(i)));
	}
	
	private static BloomFilter<Integer> bf(JavaRDD<DocumentWithFingerprint> docs) {
		List<Integer> allElements = docs.filter(i -> SparkCreateSourceDocuments.DOCS_TO_TOPIC.containsKey(i.getDocId()))
			.flatMap(i -> i.getMinHashParts().iterator())
			.distinct()
			.collect();
		
		BloomFilter<Integer> ret = BloomFilter.create(Funnels.integerFunnel(), allElements.size(), 1.0e-8);
		allElements.forEach(i -> ret.put(i));
		
		return ret;
	}
	
	@Data
	@SuppressWarnings("serial")
	private static class DeduplicationUnit implements Serializable {
		private final String id;
		private final List<Integer> hashParts;
	}

	@SuppressWarnings("serial")
	static abstract class DeduplicationStrategy implements Serializable {
		public abstract List<Integer> extract(DocumentWithFingerprint doc);
		
		public abstract int numPartitions();
		
		public Partitioner getPartitioner() {
			return new IntPartitioner(numPartitions());
		}
		
		public static DeduplicationStrategy minHashDeduplication(int numPartitions) {
			return new DeduplicationStrategy() {
				@Override
				public List<Integer> extract(DocumentWithFingerprint doc) {
					return doc.getMinHashParts();
				}

				@Override
				public int numPartitions() {
					return numPartitions;
				}
			};
		}
		
		public static DeduplicationStrategy simHashDeduplication(int numPartitions) {
			return new DeduplicationStrategy() {
				@Override
				public List<Integer> extract(DocumentWithFingerprint doc) {
					return doc.getSimHash65BitParts();
				}

				@Override
				public int numPartitions() {
					return numPartitions;
				}
			};
		}
	}
	
	@SuppressWarnings("serial")
	static class IntPartitioner extends HashPartitioner {
		public IntPartitioner(int partitions) {
			super(partitions);
		}
		
		@Override
		public int getPartition(Object key) {
			if(key == null || !(key instanceof Integer)) {
				throw new RuntimeException("I work only for ints");
			}
			
			return super.getPartition((int) key);
		}
	}
}
