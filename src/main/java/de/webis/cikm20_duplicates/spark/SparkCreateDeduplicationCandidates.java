package de.webis.cikm20_duplicates.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
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

import com.google.common.collect.ImmutableList;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import de.webis.cikm20_duplicates.util.ClientLocalDeduplication;
import de.webis.cikm20_duplicates.util.FingerPrintUtil.Fingerprinter;
import de.webis.cikm20_duplicates.util.SourceDocuments.CollectionDocumentWithTopics;
import de.webis.cikm20_duplicates.util.SourceDocuments.DocumentWithFingerprint;
import de.webis.trec_ndd.trec_collections.AnseriniCollectionReader;
import de.webis.trec_ndd.trec_collections.CollectionDocument;
import io.anserini.collection.ClueWeb09Collection.Document;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
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
			//String corpus = "cw12";
			//String corpus = "cw09-cw12";
//			String corpus = "cc-2015-11";
			String corpus = "cw09-cw12-cc-2015-11";
			DeduplicationStrategy deduplicationStrategy = DeduplicationStrategy.productionDeduplication(50000);
		
			JavaRDD<String> input = context.textFile(inputPath(corpus));
			
			exactDuplicates(input, deduplicationStrategy)
				.saveAsTextFile(path(deduplicationStrategy, corpus) + "-exact-duplicates");
			
			createDeduplicationtasks(input, deduplicationStrategy)
				.saveAsTextFile(path(deduplicationStrategy, corpus) + "-near-duplicate-tasks");
		}
	}
	
	public static String inputPath(String corpus) {
		if ("cw09-cw12".equals(corpus)) {
			return "cikm2020/document-fingerprints-final/cw*-jsonl.bzip2";
		} else if ("cw09-cw12-cc-2015-11".equals(corpus)) {
			return "cikm2020/document-fingerprints-final/{cw,cc-2015}*-jsonl.bzip2";
		}
		
		return "cikm2020/document-fingerprints-final/" + corpus +"-jsonl.bzip2";
	}
	
	public static String path(DeduplicationStrategy deduplicationStrategy, String corpus) {
		return "cikm2020/deduplication-final/" + deduplicationStrategy.name() +"/" + corpus;
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
	
	public static JavaRDD<String> toCounts(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		JavaPairRDD<Integer, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		JavaPairRDD<Integer, Integer> deduplicationTaskSizeToCount = parsedInput.groupBy(i -> i._1())
				.mapToPair(i -> new Tuple2<>(ClientLocalDeduplication.sortedList(i._2()).size(), 1))
				.reduceByKey((a,b) -> a+b);
		
		return deduplicationTaskSizeToCount
				.map(i -> "{\"groupSize\":" + i._1() + ",\"count\":" + i._2() + "}");
	}
	
	public static JavaRDD<String> toCountsOfRecursion(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		JavaPairRDD<Integer, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		return parsedInput.groupBy(i -> i._1())
				.flatMap(i -> tmpFlatCount(i));
	}
	
	public static JavaRDD<String> deduplicateWithinGroupByGroups(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		JavaPairRDD<Integer, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		return parsedInput.groupBy(i -> i._1())
				.flatMap(i -> ClientLocalDeduplication.dedup(i._2()).iterator());
	}
	
	public static JavaRDD<String> createDeduplicationtasks(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		JavaPairRDD<Integer, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		return parsedInput.groupBy(i -> i._1())
				.flatMap(i -> ClientLocalDeduplication.workingPackages(i._2()))
				.repartition(50000);
	}
	
	private static Iterator<String> tmpFlatCount(Tuple2<Integer, Iterable<Tuple2<Integer, DeduplicationUnit>>> i) {
		List<String> ret = new LinkedList<>();
		Map<String, List<Tuple2<Integer, DeduplicationUnit>>> bla = ClientLocalDeduplication.sortedList(i._2());
		int pos = 0;
		
		for(List<DeduplicationUnit> bbb : ClientLocalDeduplication.deduplicationPairs(bla)) {
			ret.add("{\"bucket\": \"" + i._1() +"-"  + (++pos) + "-recursive\", \"count\": " + bbb.size() +"}");
		}
		
		return ret.iterator();
	}


	public static JavaRDD<String> duplicationCandidatePairsFromFingerprints(JavaRDD<String> docsWithFingerprint, DeduplicationStrategy f) {
		JavaPairRDD<Integer, DeduplicationUnit> parsedInput = hashPartitionToDocument(docsWithFingerprint, f);
		
		return parsedInput.groupBy(i -> i._1())
				.flatMap(i -> emitAllPairs(i._2()))
				.filter(i -> i != null)
				.distinct();
	}

	private static List<DeduplicationUnit> sortedList(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		Set<DeduplicationUnit> uniqueIds = new HashSet<>(ImmutableList.copyOf(Iterators.transform(group.iterator(), j -> j._2())));
		List<DeduplicationUnit> ids = new ArrayList<>(uniqueIds);
		Collections.sort(ids, (a,b) -> a.id.compareTo(b.getId()));
		
		return ids;
	}
	
	@SneakyThrows
	private static Iterator<String> emitAllPairs(Iterable<Tuple2<Integer, DeduplicationUnit>> group) {
		List<DeduplicationUnit> ids = sortedList(group);
		List<String> ret = new LinkedList<>();
		
		for(int i=0; i<ids.size(); i++) {
			for(int j=i+1; j< ids.size(); j++) {
				Map<String, Object> candidate = new LinkedHashMap<>();
				candidate.put("firstId", ids.get(i).getId());
				candidate.put("secondId", ids.get(j).getId());
				candidate.put("firstFingerprintComponents", ids.get(i).getHashParts());
				candidate.put("secondFingerprintComponents", ids.get(j).getHashParts());
				
				ret.add(new ObjectMapper().writeValueAsString(candidate));
			}
		}
		
		return ret.iterator();
	}

	private static Iterator<Tuple2<Integer, DeduplicationUnit>> extractHashesToDocId(DocumentWithFingerprint doc, DeduplicationStrategy f) {
		List<Integer> hashParts = f.extract(doc);
		
		return hashParts.stream()
				.map(hash -> new Tuple2<>(hash, new DeduplicationUnit(doc.getDocId(), hashParts)))
				.iterator();
	}
	
	public static JavaRDD<DocumentWithFingerprint> duplicationCandidatesFromFingerprints(JavaRDD<String> docsWithFingerprint, String feature) {
		JavaRDD<DocumentWithFingerprint> parsedInput = docsWithFingerprint.map(i -> DocumentWithFingerprint.fromString(i));
		BloomFilter<Integer> bf = bf(parsedInput, feature);
		
		return parsedInput.filter(doc -> doc.getFingerprints().get(feature).stream().anyMatch(i -> bf.mightContain(i)));
	}
	
	private static BloomFilter<Integer> bf(JavaRDD<DocumentWithFingerprint> docs, String feature) {
		List<Integer> allElements = docs.filter(i -> SparkCreateSourceDocuments.DOCS_TO_TOPIC.containsKey(i.getDocId()))
			.flatMap(i -> i.getFingerprints().get(feature).iterator())
			.distinct()
			.collect();
		
		BloomFilter<Integer> ret = BloomFilter.create(Funnels.integerFunnel(), allElements.size(), 1.0e-8);
		allElements.forEach(i -> ret.put(i));
		
		return ret;
	}
	
	@Data
	@NoArgsConstructor
	@AllArgsConstructor
	@SuppressWarnings("serial")
	public static class DeduplicationUnit implements Serializable {
		private String id;
		private List<Integer> hashParts;
	}

	@SuppressWarnings("serial")
	public static abstract class DeduplicationStrategy implements Serializable {
		public final List<Integer> extract(DocumentWithFingerprint doc) {
			return doc.getFingerprints().get(name());
		}
		
		public abstract int numPartitions();
		
		public abstract String name();
		
		public Partitioner getPartitioner() {
			return new IntPartitioner(numPartitions());
		}
		
		public Partitioner getStringPartitioner() {
			return new StringPartitioner(numPartitions());
		}
		
		public static DeduplicationStrategy minHashDeduplication(int numPartitions) {
			return new DeduplicationStrategy() {
				@Override
				public int numPartitions() {
					return numPartitions;
				}

				@Override
				public String name() {
					return "MinHashWithJavaHash";
				}
			};
		}

		public static DeduplicationStrategy productionDeduplication(int numPartitions) {
			return new DeduplicationStrategy() {
				@Override
				public String name() {
					return "64BitK3SimHashThreeAndFiveGramms";
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
				public String name() {
					return "64BitK3SimHashOneGramms";
				}

				@Override
				public int numPartitions() {
					return numPartitions;
				}
			};
		}
	}
	
	@SuppressWarnings("serial")
	static class StringPartitioner extends HashPartitioner {
		public StringPartitioner(int partitions) {
			super(partitions);
		}
		
		@Override
		public int getPartition(Object key) {
			if(key == null || !(key instanceof String)) {
				throw new RuntimeException("I work only for Strings");
			}
			
			return super.getPartition((String) key);
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

	public static JavaRDD<String> exactDuplicates(JavaRDD<String> input, DeduplicationStrategy dedupStrategy) {
		JavaPairRDD<String, String> tmp = input.map(i -> DocumentWithFingerprint.fromString(i))
				.mapToPair(i -> new Tuple2<String, String>(dedupStrategy.extract(i).toString(), i.getDocId()))
				.repartitionAndSortWithinPartitions(dedupStrategy.getStringPartitioner());
	
		return tmp
				.groupBy(i -> i._1())
				.map(i -> toExactRepresentationOrNull(i))
				.filter(i -> i != null);
	}

	@SneakyThrows
	private static String toExactRepresentationOrNull(Tuple2<String, Iterable<Tuple2<String, String>>> i) {
		List<String> ret = new ArrayList<>(new HashSet<>(ImmutableList.copyOf(Iterators.transform(i._2.iterator(), j -> j._2()))));
		Collections.sort(ret);
		
		if(ret.size() <= 1) {
			return null;
		}
		
		return "{\"equivalentDocuments\": "+ new ObjectMapper().writeValueAsString(ret) +",\"hash\":"+ i._1() +"}";
	}
	
}
