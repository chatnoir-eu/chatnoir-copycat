package de.webis.cikm20_duplicates.spark.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction;
import de.webis.cikm20_duplicates.spark.SparkRelevanceTransferDataConstruction.RelevanceTransferPair;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkAggregateKnowledgeTransferBetweenCrawls {

	@SneakyThrows
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> nearDuplicatesWithoutExactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates/part*/part*");
			JavaRDD<String> exactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates");
			
			Map<String, Long> data = aggregateKnowledgeTransfer(nearDuplicatesWithoutExactDuplicates, exactDuplicates);
			String dataJson = new ObjectMapper().writeValueAsString(data);
			context.parallelize(Arrays.asList(dataJson), 1).saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-relevance-transfer-aggregations");
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
	
	public static List<String> labels(String id1, String id2) {
		List<String> ret = internalLabels(id1, id2);
		
		if(ret.isEmpty()) {
			return ret;
		}
		
		List<String> bla = new ArrayList<>(ret);
		
		List<RelevanceTransferPair> tmp = SparkRelevanceTransferDataConstruction.possibleRelevanceTransferPairsWithoutURLsFromTo(id1, id2, 0);
		tmp.addAll(SparkRelevanceTransferDataConstruction.possibleRelevanceTransferPairsWithoutURLsFromTo(id2, id1, 0));
		ret = new ArrayList<>(ret);
		
		for(RelevanceTransferPair relevanceTransfer: tmp) {
			for(String a: bla) {
				ret.add(a + "---relevance---" + relevanceTransfer.getRelevanceLabel());
				ret.add(a + "---topic---" + relevanceTransfer.getTopic() + "---relevance---" + relevanceTransfer.getRelevanceLabel());
			}
		}
		
		return ret; 
	}
	
	public static List<String> internalLabels(String id1, String id2) {
		if(id1 == null|| id2 == null) {
			return Collections.emptyList();
		}
		
		if((isCw09(id1) && isCw12(id2)) || (isCw09(id2) && isCw12(id1))) {
			return Arrays.asList("cw09-to-cw12");
		} else if((isCw09(id1) && isCC15(id2)) || (isCw09(id2) && isCC15(id1))) {
			return Arrays.asList("cw09-to-cc15");
		} else if((isCw12(id1) && isCC15(id2)) || (isCw12(id2) && isCC15(id1)) ) {
			return Arrays.asList("cw12-to-cc15");
		}
		
		return Collections.emptyList();
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

	public static Map<String, Long> aggregateKnowledgeTransfer(JavaRDD<String> nearDuplicatesWithoutExactDuplicates, JavaRDD<String> exactDuplicates) {
		JavaPairRDD<String, Long> a = nearDuplicatesWithoutExactDuplicates
				.flatMap(i -> labelsFromNearDuplicates(i).iterator())
				.mapToPair(i -> new Tuple2<>(i, 1l))
				.reduceByKey((i,j)-> i+j);
		
		JavaPairRDD<String, Long> b = exactDuplicates
				.flatMap(i -> labelsFromExactDuplicates(i).iterator())
				.mapToPair(i -> new Tuple2<>(i, 1l))
				.reduceByKey((i,j)-> i+j);
		
		return a.union(b).reduceByKey((i,j)-> i+j)
				.collectAsMap();
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> labelsFromNearDuplicates(String src) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		String firstId = (String) parsed.get("firstId");
		String secondId = (String) parsed.get("secondId");
		
		return labels(firstId, secondId);
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static List<String> labelsFromExactDuplicates(String src) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		List<String> ids = (List<String>) parsed.get("equivalentDocuments");
		List<String> ret = new LinkedList<>();
		
		for(int i=0; i<ids.size(); i++) {
			for(int j=i+1; j<ids.size(); j++) {
				ret.addAll(labels(ids.get(i), ids.get(j)));
			}
		}
		
		return ret;
	}
}
