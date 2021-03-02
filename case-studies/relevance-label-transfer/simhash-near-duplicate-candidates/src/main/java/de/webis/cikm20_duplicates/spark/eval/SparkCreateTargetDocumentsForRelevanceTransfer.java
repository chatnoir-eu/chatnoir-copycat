package de.webis.cikm20_duplicates.spark.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.spark.SparkCreateSourceDocuments;
import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction;
import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction.RelevanceTransferPair;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkCreateTargetDocumentsForRelevanceTransfer {
	@SneakyThrows
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> nearDuplicatesWithoutExactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates-csv-distinct");
			JavaRDD<String> exactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates");
			
			Map<String, List<String>> data = aggregateKnowledgeTransfer(nearDuplicatesWithoutExactDuplicates, exactDuplicates);
			String dataJson = new ObjectMapper().writeValueAsString(data);
			context.parallelize(Arrays.asList(dataJson), 1).saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-relevance-transfer-target-ids");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/eval/aggregate-knowledge-transfer");

		return new JavaSparkContext(conf);
	}
	
	public static Map<String, Long> aggregate(Map<String, Long> a, Map<String, Long> b) {
		Map<String, Long> ret = new HashMap<>();
		addAllValues(a, ret);
		addAllValues(b, ret);
		
		return ret;
	}

	private static void addAllValues(Map<String, Long> a, Map<String, Long> b) {
		if(a!= null && b != null) {
			for(Map.Entry<String, Long> kv: a.entrySet()) {
				if(!b.containsKey(kv.getKey())) {
					b.put(kv.getKey(), 0l);
				}
				
				b.put(kv.getKey(), b.get(kv.getKey()) + kv.getValue());
			}
		}
	}
	
	public static Map<String, List<String>> labels(String id1, String id2) {
		Map<String, List<String>> ret = internalLabels(id1, id2);
		
		if(ret.isEmpty()) {
			return ret;
		}
		
		List<String> bla = new ArrayList<>(ret.keySet());
		
		List<RelevanceTransferPair> tmp = SparkRelevanceTransferDataConstruction.possibleRelevanceTransferPairsWithoutURLsFromTo(id1, id2, 0);
		tmp.addAll(SparkRelevanceTransferDataConstruction.possibleRelevanceTransferPairsWithoutURLsFromTo(id2, id1, 0));
		ret = new HashMap<>(ret);
		
		for(RelevanceTransferPair relevanceTransfer: tmp) {
			for(String a: bla) {
				String key = a + "---topic---" + relevanceTransfer.getTopic() + "---source-doc---" + relevanceTransfer.getSrcId() + "---relevance---" + relevanceTransfer.getRelevanceLabel();
				
				if(!ret.containsKey(key)) {
					ret.put(key, new ArrayList<>());
				}

				ret.put(key, retainOnlyAlphanumericalLowestId(ret.get(key), Arrays.asList(relevanceTransfer.getTargetId())));
			}
		}
		
		return ret; 
	}
	
	private static List<String> retainOnlyAlphanumericalLowestId(List<String> a, List<String> b) {
		List<String> ret = new ArrayList<>();
		
		if(a != null) {
			ret.addAll(a);
		}
		if( b != null) {
			ret.addAll(b);
		}
		
		return ret.stream().limit(2).collect(Collectors.toList());
	}
	
	public static Map<String, List<String>> internalLabels(String id1, String id2) {
		Map<String, List<String>> ret = new HashMap<>();
		if(id1 == null|| id2 == null) {
			return ret;
		}
		
		if((isCw09(id1) && isCw12(id2)) || (isCw09(id2) && isCw12(id1))) {
			ret.put("cw09-to-cw12", Collections.emptyList());
		} else if((isCw09(id1) && isCC15(id2)) || (isCw09(id2) && isCC15(id1))) {
			ret.put("cw09-to-cc15", Collections.emptyList());
		} else if((isCw12(id1) && isCC15(id2)) || (isCw12(id2) && isCC15(id1)) ) {
			ret.put("cw12-to-cc15", Collections.emptyList());
		}
		
		return ret;
	}
	
	private static boolean isCC15(String id) {
		return !isCw09(id) && !isCw12(id);
	}
	
	private static boolean isCw09(String id) {
		return id != null && id.startsWith("clueweb09");
	}
	
	private static boolean isCw12(String id) {
		return id != null && id.startsWith("clueweb12");
	}

	public static Map<String, List<String>> aggregateKnowledgeTransfer(JavaRDD<String> nearDuplicatesWithoutExactDuplicates, JavaRDD<String> exactDuplicates) {
		JavaPairRDD<String, List<String>> a = nearDuplicatesWithoutExactDuplicates
				.flatMap(i -> labelsFromNearDuplicates(i).entrySet().iterator())
				.mapToPair(i -> new Tuple2<>(i.getKey(), i.getValue()))
				.reduceByKey((i,j)-> retainOnlyAlphanumericalLowestId(i,j));
		
		JavaPairRDD<String, List<String>> b = exactDuplicates
				.flatMapToPair(i -> labelsFromExactDuplicates(i).iterator())
				.reduceByKey((i,j)-> retainOnlyAlphanumericalLowestId(i,j));
		
		return a.union(b).reduceByKey((i,j)-> retainOnlyAlphanumericalLowestId(i,j))
				.collectAsMap();
	}
	
	private static Map<String, List<String>> labelsFromNearDuplicates(String src) {
		if(1 != StringUtils.countMatches(src, ",")) {
			throw new RuntimeException("Could not handle pair: '" + src + "'.");
		}
		
		String firstId = StringUtils.substringBefore(src, ",");
		String secondId = StringUtils.substringAfter(src, ",");
		
		return labels(firstId, secondId);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<Tuple2<String, List<String>>> labelsFromExactDuplicates(String src) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		Set<String> ids = new HashSet<>((List<String>) parsed.get("equivalentDocuments"));
		Map<String, List<RelevanceTransferPair>> idToTransfer = new HashMap<>();
		long cw09 = 0, cw12 = 0, cc15 = 0;
		List<String> cw09Ids = new ArrayList<>();
		List<String> cw12Ids = new ArrayList<>();
		List<String> cc15Ids = new ArrayList<>();
		
		for(String id: ids) {
			if(isCw09(id)) {
				cw09++;
				cw09Ids.add(id);
			} else if(isCw12(id)) {
				cw12++;
				cw12Ids.add(id);
			} else {
				cc15++;
				cc15Ids.add(id);
			}
			
			Set<String> sourceTopics = SparkCreateSourceDocuments.DOCS_TO_TOPIC.getOrDefault(id, Collections.EMPTY_SET);
			List<RelevanceTransferPair> tmp = new ArrayList<>();
			
			for(String sourceTopic: sourceTopics) {
				RelevanceTransferPair rt = RelevanceTransferPair.transferPairWithoutChatnoirId(src, null, sourceTopic, 0);
				if(rt != null) {
					tmp.add(rt);
				}
			}
			
			idToTransfer.put(id, tmp);
		}
		
		List<Tuple2<String, List<String>>> ret = new ArrayList<>();
		
		for(Map.Entry<String, List<RelevanceTransferPair>> docAndRels: idToTransfer.entrySet()) {
			for(RelevanceTransferPair relevanceTransfer: docAndRels.getValue()) {
				if(isCw09(docAndRels.getKey()) && cw12 > 0) {
					ret.add(new Tuple2<>("cw09-to-cw12---topic---" + relevanceTransfer.getTopic() + "---source-doc---" + relevanceTransfer.getSrcId() + "---relevance---" + relevanceTransfer.getRelevanceLabel(), retainOnlyAlphanumericalLowestId(cw12Ids, null)));
				}
				
				if(isCw09(docAndRels.getKey()) && cc15 > 0) {
					ret.add(new Tuple2<>("cw09-to-cc15---topic---" + relevanceTransfer.getTopic() + "---source-doc---" + relevanceTransfer.getSrcId() + "---relevance---" + relevanceTransfer.getRelevanceLabel(), retainOnlyAlphanumericalLowestId(cc15Ids, null)));
				}
				
				if(isCw12(docAndRels.getKey()) && cc15 > 0) {
					ret.add(new Tuple2<>("cw12-to-cc15---topic---" + relevanceTransfer.getTopic() + "---source-doc---" + relevanceTransfer.getSrcId() + "---relevance---" + relevanceTransfer.getRelevanceLabel(), retainOnlyAlphanumericalLowestId(cc15Ids, null)));
				}
			}
			
		}

		return ret;
	}
}
