package de.webis.cikm20_duplicates.spark;

import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.fasterxml.jackson.databind.ObjectMapper;

import lombok.SneakyThrows;

public class SparkCombineIntermediateResults {
	
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			JavaRDD<String> input = context.textFile("cikm2020/near-duplicate-tasks-cw09-cw12");
			
			combineIntermediateResults(input, 5000)
				.saveAsTextFile("cikm2020/results/test-01");
		}
	}

	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/candidates");

		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> combineIntermediateResults(JavaRDD<String> inputRDD, int partitions) {
		return inputRDD
				.map(i -> toCsv(i))
				.distinct()
				.sortBy(i -> i, Boolean.TRUE, partitions);
	}

	@SneakyThrows
	@SuppressWarnings("unchecked")
	private static String toCsv(String json) {
		Map<String, Object> ret = new ObjectMapper().readValue(json, Map.class);
		String firstId = (String) ret.get("firstId");
		String secondId = (String) ret.get("secondId");
		Integer hemmingDistance = (Integer) ret.get("hemmingDistance");
		
		if(firstId == null|| secondId == null || hemmingDistance == null) {
			throw new IllegalArgumentException("");
		}
		
		return firstId + "," + secondId + "," + hemmingDistance;
	}
}
