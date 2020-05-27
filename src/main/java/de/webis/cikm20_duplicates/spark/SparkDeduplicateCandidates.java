package de.webis.cikm20_duplicates.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.util.ClientLocalDeduplication;
import de.webis.cikm20_duplicates.util.ClientLocalDeduplication.DeduplicationTask;
import scala.Tuple2;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkDeduplicateCandidates {
//	public static void main(String[] args) {
//		try (JavaSparkContext context = context()) {
//			context.textFile("cikm2020/near-duplicate-tasks-cw09-cw12")
//				.map(i-> DeduplicationTask.fromString(i))
//				.flatMap(i -> ClientLocalDeduplication.fullDeduplication(i.getEntries()).iterator())
//				.saveAsTextFile("cikm2020/deduplication/near-duplicates/cw09-cw12");
//		}
//	}
	
	public static void main(String[] args) {
		String corpus = "cw09-cw12";
		try (JavaSparkContext context = context()) {
			JavaPairRDD<Integer, Integer> deduplicationTaskSizeToCount = context.textFile(SparkCreateDeduplicationCandidates.inputPath(corpus))
				.mapToPair(i-> new Tuple2<>(DeduplicationTask.fromString(i).getEntries().size(), 1))
				.reduceByKey((a,b) -> a+b);
			
			deduplicationTaskSizeToCount
				.map(i -> "{\"groupSize\":" + i._1() + ",\"count\":" + i._2() + "}")
				.saveAsTextFile("cikm2020/document-fingerprints-final/deduplication-task-size-to-count/" + corpus);
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/deduplication/near-duplicates");

		return new JavaSparkContext(conf);
	}
}
