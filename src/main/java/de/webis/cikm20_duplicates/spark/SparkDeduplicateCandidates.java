package de.webis.cikm20_duplicates.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.cikm20_duplicates.util.ClientLocalDeduplication;
import de.webis.cikm20_duplicates.util.ClientLocalDeduplication.DeduplicationTask;

/**
 * 
 * @author Maik Fröbe
 *
 */
public class SparkDeduplicateCandidates {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			context.textFile("cikm2020/near-duplicate-tasks-cw09-cw12")
				.map(i-> DeduplicationTask.fromString(i))
				.flatMap(i -> ClientLocalDeduplication.fullDeduplication(i.getEntries()).iterator())
				.saveAsTextFile("cikm2020/deduplication/near-duplicates/cw09-cw12");
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/deduplication/near-duplicates");

		return new JavaSparkContext(conf);
	}
}
