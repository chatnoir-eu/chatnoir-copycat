package de.webis.copycat_spark.spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkMakeDeduplicatedPairsUnique {
	@SneakyThrows
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus: new String[] {"cw09-cw12-cc-2015-11", "cc-2017-04", "cw09-cw12"}) {
				JavaRDD<String> nearDuplicatesWithoutExactDuplicates = context.textFile(inputPath(corpus));
				nearDuplicatesWithoutExactDuplicates.map(i -> nearDuplicatePair(i))
					.distinct()
					.saveAsTextFile("cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/" + corpus + "-near-duplicates-without-exact-duplicates-csv-distinct");
				
			}
		}
	}
	
	public static JavaRDD<String> distinctNearDuplicatePairs(JavaRDD<String> nearDuplicateJSONL) {
		return nearDuplicateJSONL
				.mapToPair(i -> new Tuple2<>(nearDuplicatePair(i), i))
				.groupByKey().map(i -> combineRedundantNearDuplicateEntries(i));
	}
	
	private static String combineRedundantNearDuplicateEntries(Tuple2<String, Iterable<String>> i) {
		Iterator<String> values = i._2.iterator(); 
		String first = values.next();
		values.forEachRemaining(rem -> {
			if(rem != null && !rem.equals(first)) {
				throw new RuntimeException("Could not handle '" + first + "' vs '" + rem + "'.");
			}
		});
		
		return first;
	}

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static String nearDuplicatePair(String src) {
		Map<String, Object> parsed = new ObjectMapper().readValue(src, Map.class);
		String firstId = (String) parsed.get("firstId");
		String secondId = (String) parsed.get("secondId");
		List<String> ids = new ArrayList<>(Arrays.asList(firstId, secondId));
		Collections.sort(ids);
		
		return ids.get(0) + "," + ids.get(1);
	}
	
	private static String inputPath(String corpus) {
		if("cw09-cw12-cc-2015-11".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-cc-2015-11-near-duplicates-without-exact-duplicates/part*/part*";
		} else if("cw09-cw12".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cw09-cw12-near-duplicates-without-exact-duplicates";
		} else if ("cc-2017-04".equals(corpus)) {
			return "cikm2020/deduplication-final/64BitK3SimHashThreeAndFiveGramms/cc-2017-04-near-duplicates-without-exact-duplicates/part*/part*";
		}
		
		throw new RuntimeException("Invalid corpus: " + corpus);
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/distinct-deduplication-pairs");

		return new JavaSparkContext(conf);
	}
}
