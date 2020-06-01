package de.webis.cikm20_duplicates.spark.eval;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkIdentifyDocumentsInTargetCrawl {
//	@SneakyThrows
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			JavaRDD<String> nearDuplicatesWithoutExactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates-csv-distinct");
//			JavaRDD<String> exactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-exact-duplicates");
//			
//			JavaRDD<Tuple2<String, String>> data = DocumentsInTargetCrawl(nearDuplicatesWithoutExactDuplicates, exactDuplicates);
//			data.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-documents-intarget-crawl");
//		}
//	}
	
	@SneakyThrows
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> nearDuplicatesWithoutExactDuplicates = context.textFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates-csv-distinct")
					.distinct(100);
			
			JavaPairRDD<String, Long> sourceToTarget = nearDuplicatesWithoutExactDuplicates.mapToPair(i -> constructSourceToTargetPair(i));
			Map<String, Long> ret = sourceToTarget.reduceByKey((a,b) -> a+b).collectAsMap();
			
			context.parallelize(Arrays.asList(new ObjectMapper().writeValueAsString(ret)))
				.repartition(1)
				.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-documents-intarget-crawl-aggregation.json");
		}
	}
	
	private static Tuple2<String, Long> constructSourceToTargetPair(String i) {
		String target = StringUtils.substringBetween(i, "(", ",");
		String sourceId = StringUtils.substringBetween(i, ",", ")");
		
		String source = null;
		
		if(isCw09(sourceId)) {
			source = "cw09";
		} else if (isCw12(sourceId)) {
			source = "cw12";
		} else {
			throw new RuntimeException("I can not Handle: '" + i + "'.");
		}
		
		return new Tuple2<>(source + "-to-" + target, 1l);
	}

	private static JavaRDD<Tuple2<String, String>> DocumentsInTargetCrawl(JavaRDD<String> nearDuplicatesWithoutExactDuplicates, JavaRDD<String> exactDuplicates) {
		JavaRDD<Tuple2<String, String>> a = nearDuplicatesWithoutExactDuplicates.flatMap(i -> labelsFromNearDuplicates(i));
		JavaRDD<Tuple2<String, String>> b = exactDuplicates.flatMap(i -> labelsFromExactDuplicates(i));
		
		return a.union(b);
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/eval/aggregate-knowledge-transfer");

		return new JavaSparkContext(conf);
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
	
	private static Iterator<Tuple2<String, String>> labelsFromNearDuplicates(String src) {
		if(1 != StringUtils.countMatches(src, ",")) {
			throw new RuntimeException("Could not handle pair: '" + src + "'.");
		}
		
		String firstId = StringUtils.substringBefore(src, ",");
		String secondId = StringUtils.substringAfter(src, ",");
		
		if((isCw09(firstId) && isCw09(secondId)) || (isCw12(firstId)) && (isCw12(secondId)) || (isCC15(firstId) && isCC15(secondId))) {
			return Collections.emptyIterator();
		}
		
		String srcId = null;
		
		if(isCw09(firstId)) {
			srcId = firstId;
		} else if (isCw09(secondId)) {
			srcId = secondId;
		} else if(isCw12(firstId)) {
			srcId = firstId;
		} else if(isCw12(secondId)) {
			srcId = secondId;
		}
		
		String target = "cw12";
		
		if(isCC15(firstId) || isCC15(secondId)) {
			target = "cc-2015-11";
		}
		
		return Arrays.asList(new Tuple2<>(target, srcId)).iterator();
	}
	
	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static Iterator<Tuple2<String, String>> labelsFromExactDuplicates(String src) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		Set<String> ids = new HashSet<>((List<String>) parsed.get("equivalentDocuments"));
		
		Set<String> cw09Ids = new HashSet<>();
		Set<String> cw12Ids = new HashSet<>();
		Set<String> cc15Ids = new HashSet<>();
		
		for(String id: ids) {
			if(isCw09(id)) {
				cw09Ids.add(id);
			} else if(isCw12(id)) {
				cw12Ids.add(id);
			} else {
				cc15Ids.add(id);
			}
		}
		
		List<Tuple2<String, String>> ret = new ArrayList<>();
		
		for(String cw09Id: cw09Ids) {
			if(!cw12Ids.isEmpty()) {
				ret.add(new Tuple2<>("cw12", cw09Id));
			}
			
			if(!cc15Ids.isEmpty()) {
				ret.add(new Tuple2<>("cc-2015-11", cw09Id));
			}
		}
		
		for(String cw12Id: cw12Ids) {
			if(!cc15Ids.isEmpty()) {
				ret.add(new Tuple2<>("cc-2015-11", cw12Id));
			}
		}
		
		return ret.iterator();
	}
}
