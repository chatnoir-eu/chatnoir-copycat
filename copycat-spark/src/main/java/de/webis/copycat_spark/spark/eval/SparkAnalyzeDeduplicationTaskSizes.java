package de.webis.copycat_spark.spark.eval;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import de.webis.copycat_spark.spark.SparkCreateDeduplicationCandidates;
import de.webis.copycat_spark.spark.SparkCreateDeduplicationCandidates.DeduplicationStrategy;

public class SparkAnalyzeDeduplicationTaskSizes {
	public static void main(String[] args) {
		try (JavaSparkContext context = context()) {
			for(String corpus: new String[] {"cw09", "cw12", "cw09-cw12", "cc-2015-11", "cw09-cw12-cc-2015-11"}) {
				DeduplicationStrategy deduplicationStrategy = DeduplicationStrategy.productionDeduplication(50000);

				JavaRDD<String> input = context.textFile(SparkCreateDeduplicationCandidates.inputPath(corpus));
				SparkCreateDeduplicationCandidates.toCounts(input, deduplicationStrategy)
					.saveAsTextFile("cikm2020/document-fingerprints-final/deduplication-task-size-to-count/" + corpus);
			}
		}
	}
	
	private static JavaSparkContext context() {
		SparkConf conf = new SparkConf(true);
		conf.setAppName("cikm2020/eval/deduplication-task-sizes");

		return new JavaSparkContext(conf);
	}
}
