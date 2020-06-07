package de.webis.cikm20_duplicates.spark;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class SparkCreateIdsToRemove {
	
	@SuppressWarnings("serial")
	public static KeepId 
		CLUEWEB09 = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return id != null && id.startsWith("clueweb09");
			}
		},
		
		CLUEWEB12 = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return id != null && id.startsWith("clueweb12");
			}
		},
		
		COMMON_CRAWL = new KeepId() {
			@Override
			public boolean keepId(String id) {
				return id != null && !id.startsWith("clueweb12") && !id.startsWith("clueweb09");
			}
		};
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus: new String[] {"cw09", "cw12", "cc-2015-11", "cc-2017-04"}) {
//				JavaRDD<String> nearDuplicates = context.textFile(nearDupPath(corpus));
//				JavaRDD<String> exactDuplicates = context.textFile(exactDupPath(corpus));
//				
//				idsToRemove(nearDuplicates, exactDuplicates, idsToKeep(corpus))
//					.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-ids-to-remove");
//			}
//		}
//	}
		
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus: new String[] {"cw09-cw12-cc15"}) {
//				JavaRDD<String> nearDuplicates = context.textFile(nearDupPath(corpus));
//				JavaRDD<String> exactDuplicates = context.textFile(exactDupPath(corpus));
//					
//				idsToRemoveNonDistinct(nearDuplicates, exactDuplicates, idsToKeep(corpus))
//					.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-ids-to-remove-ATTENTION-NON-DISTINCT");
//			}
//		}
//	}
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			JavaRDD<String> toDistinct = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cc-2017-04-ids-to-remove-ATTENTION-NON-DISTINCT");
//				
//			toDistinct.distinct()
//				.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cc-2017-04-ids-to-remove");
//
//		}
//	}
	
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			for(String corpus: new String[] {"cw09", "cw12", "cc-2015-11", "cc-2017-04"}) {
//				JavaRDD<String> idsToRemove = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-ids-to-remove");
//				
//				idsToRemove.repartition(numPartitions(corpus))
//					.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-ids-to-remove-bzip2", BZip2Codec.class);
//			}
//		}
//	}

	@SneakyThrows
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			Map<String, Long> corpusToExactDuplicateGroups = new HashMap<>();
			for(String corpus: new String[] {"cw09", "cw12", "cc-2015-11", "cc-2017-04", "cw09-cw12-cc15"}) {
				JavaRDD<String> exactDuplicates = context.textFile(exactDupPath(corpus));
				KeepId keepId = idsToKeep(corpus);
				
				long count = exactDuplicates.map(i -> idsInExactDuplicates(i, keepId)).filter(i -> i > 0).count();
				
				corpusToExactDuplicateGroups.put(corpus, count);
			}
			
			context.parallelize(Arrays.asList(new ObjectMapper().writeValueAsString(corpusToExactDuplicateGroups)),1)
				.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/count-of-exact-duplicate-groups-per-corpus");
		}
	}
		
	private static int numPartitions(String corpus) {
		if(Arrays.asList("cw09", "cw12").contains(corpus)) {
			return 1;
		} else if(Arrays.asList("cc-2015-11", "cc-2017-04").contains(corpus)) {
			return 10;
		}

		throw new RuntimeException("Can not handle: " + corpus);
	}

	@SuppressWarnings("serial")
	private static KeepId idsToKeep(String corpus) {
		if("cw09".equals(corpus)) {
			return CLUEWEB09;
		} else if ("cw12".equals(corpus)) {
			return CLUEWEB12;
		} else if ("cc-2015-11".equals(corpus)) {
			return COMMON_CRAWL;
		} else if ("cc-2017-04".equals(corpus)) {
			return COMMON_CRAWL;
		} else if ("cw09-cw12-cc15".equals(corpus)) {
			return new KeepId() {
				@Override
				public boolean keepId(String id) {
					return true;
				}
			};
		} else {
			throw new RuntimeException("Can not handle: " + corpus);
		}
	}

	private static String exactDupPath(String corpus) {
		if("cw09-cw12-cc15".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates";
		} else if("cw09".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates";
		} else if ("cw12".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates";
		} else if ("cc-2015-11".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates";
		} else if ("cc-2017-04".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cc-2017-04-exact-duplicates";
		} else {
			throw new RuntimeException("Can not handle: " + corpus);
		}
	}
	
	private static String nearDupPath(String corpus) {
		if("cw09-cw12-cc15".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates/part*/part*";
		}
		if("cw09".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates/part*/part*";
		} else if ("cw12".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates/part*/part*";
		} else if ("cc-2015-11".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates/part*/part*";
		} else if ("cc-2017-04".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cc-2017-04-near-duplicates-without-exact-duplicates/part*/part*";
		} else {
			throw new RuntimeException("Can not handle: " + corpus);
		}
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/ids-to-remove");

		return new JavaSparkContext(conf);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> docsToRemoveFromNearDuplicates(String src, KeepId keepId) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		String firstId = (String) parsed.get("firstId");
		String secondId = (String) parsed.get("secondId");
		
		return idsToRemove(Arrays.asList(firstId, secondId), keepId);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> docsToRemoveFromExactDuplicates(String src, KeepId keepId) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		
		return idsToRemove(ids, keepId);
	}

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static int idsInExactDuplicates(String src, KeepId keepId) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		ids = ids.stream().filter(id -> keepId.keepId(id)).collect(Collectors.toList());
		
		if(ids == null || ids.isEmpty()) {
			return 0;
		} else {
			return 1;
		}
	}
	
	public static JavaRDD<String> idsToRemoveNonDistinct(JavaRDD<String> nearDuplicates, JavaRDD<String> exactDuplicates, KeepId keepId) {
		nearDuplicates = nearDuplicates.flatMap(i -> docsToRemoveFromNearDuplicates(i, keepId).iterator())
				.filter(i -> i != null);
		exactDuplicates = exactDuplicates.flatMap(i -> docsToRemoveFromExactDuplicates(i, keepId).iterator())
				.filter(i -> i != null);
		
		return nearDuplicates.union(exactDuplicates);
	}
	
	public static JavaRDD<String> idsToRemove(JavaRDD<String> nearDuplicates, JavaRDD<String> exactDuplicates, KeepId keepId) {
		return idsToRemoveNonDistinct(nearDuplicates, exactDuplicates, keepId)
				.distinct();
	}

	public static List<String> idsToRemove(List<String> ids, KeepId keepId) {
		if(ids == null || ids.size() < 2) {
			return Collections.emptyList();
		}
		
		ids = new ArrayList<>(new HashSet<>(ids).stream()
				.filter(i -> keepId.keepId(i))
				.collect(Collectors.toList()));
		if(ids.size() < 2) {
			return Collections.emptyList();
		}
		
		Collections.sort(ids);
		ids.remove(0);
		
		return ids;
	}
	
	public static interface KeepId extends Serializable {
		public boolean keepId(String id);
	}
}
