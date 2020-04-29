package de.webis.cikm20_duplicates.spark.eval;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.spark.SparkCanonicalLinkGraphExtraction.CanonicalLinkGraphEdge;
import lombok.SneakyThrows;
import scala.Tuple2;

public class SparkAnalyzeCanonicalLinkGraph {

	private static final String DIR = "cikm2020/canonical-link-graph/";
	
	public static void main(String[] args) {
		String[] corpora = new String[] {"cw12", "cw09"};
		
		try (JavaSparkContext context = context()) {
			for(String corpus : corpora) {
				JavaRDD<String> input = context.textFile(DIR + corpus);
				
				duplicateGroupCounts(input)
					.saveAsTextFile(DIR + corpus + "-duplicate-group-counts");
				duplicateGroupCountsPerDomain(input)
					.saveAsTextFile(DIR + corpus + "-duplicate-group-counts-per-domain");
			}
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/analyze-canonical-link-graph");
	
		return new JavaSparkContext(conf);
	}
	
	public static JavaRDD<String> duplicateGroupCounts(JavaRDD<String> input) {
		JavaPairRDD<String, Integer> groupSizeToOne = urlToCount(input)
				.mapToPair(i -> new Tuple2<>(i._2() + "", 1));

		return groupSizeToOne.reduceByKey((a,b) -> a+b)
				.map(i -> "{\"groupSize\": " + i._1() + ", \"count\": " + i._2() +"}");
	}

	private static JavaPairRDD<String, Integer> urlToCount(JavaRDD<String> input) {
		return input.map(i -> CanonicalLinkGraphEdge.fromString(i))
				.mapToPair(i -> new Tuple2<>(i.getCanonicalLink().toString(), 1))
				.reduceByKey((a,b) -> a+b)
				.filter(i -> i._2() > 1);
	}
	
	public static JavaRDD<String> duplicateGroupCountsPerDomain(JavaRDD<String> input) {
		return urlToCount(input)
				.mapToPair(i -> new Tuple2<String, Tuple2<Integer, Integer>>(hostFromUrl(i._1()), new Tuple2<>(i._2(), 1)))
				.reduceByKey((a,b) -> new Tuple2<Integer, Integer>(a._1() + b._1(), a._2() + b._2()))
				.map(i -> "{\"domain\":\"" + i._1() + "\",\"groups\":" + i._2()._2() + ",\"documents\":" + i._2()._1() + "}");
	}
	
	@SneakyThrows
	static String hostFromUrl(String url) {
		try {
			return new URI(url).getHost();
		} catch (URISyntaxException e) {
			url = url.substring(0,  e.getIndex());
			return new URI(url).getHost();
		}
	}
}
